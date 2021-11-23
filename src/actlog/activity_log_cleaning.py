'''
Created on Nov 18, 2021

@author: paepcke
'''
import csv
import getpass
import gzip
import os
import re

from logging_service import LoggingService
from pymysql_utils.pymysql_utils import MySQLDB
from prompt_toolkit.input import typeahead


ID_POS = 0
EMPLID_POS = 1
IP_ADDRESS_POS = 2
CALLER_POS = 3
ACTION_POS = 4
KEY_PARAMETER_POS = 5
ENVIRONMENT_POS = 6
OUTPUT_POS = 7
BROWSER_POS = 8
CREATED_AT_POS = 9
UPDATED_AT_POS = 10

class ActivityLogCleaner(object):
    '''
    classdocs
    '''

    DB_BATCH_SIZE = 10000
    DB_NAME = 'activity_log'

    STRM_LEN = 4

    caller_pat = re.compile(r"")
    
    pins_and_enroll_hist_pat = re.compile(b"pinned:{([^}]*)}, course_history_ids:([^\]]*])")
    p_history_pat = re.compile(b"#<Enrollment (STRM: [0-9]{4}, CRSE_ID: [0-9]{6})")
    # Exctract 'selected course' id from 
    #   {selected_course:111846, name:BIO42}
    crse_selection_pat = re.compile(r'[^:]*:([^,]*).*')
    
    # Search term pattern: extract search term from
    #    '{search_term_accumulator:cs 1}
    # i.e. extract 'cs 1':
    search_term_pat = re.compile(r'[^:]*:([^}]*)')
    
    #------------------------------------
    # Constructor 
    #-------------------

    def __init__(self, activity_log_path, unittesting=False):
        '''
        Constructor
        '''

        # The data for each action type is accumulated
        # in a separate buffer, and pushed down into a
        # corresponding table when the buffer is full. 
        # The following are the buffers:
        
        # Holds one tuple for each action:
        self.activity_buf = BufferClass('activity_buf', self.DB_BATCH_SIZE)
         
        # Three buffers around pinning:
        # two for the action of pinning and unpinning...:
        self.pins_buf = BufferClass('pins_buf', int(self.DB_BATCH_SIZE / 2))
        self.unpins_buf = BufferClass('unpins_buf', int(self.DB_BATCH_SIZE / 2))
        # ... the third for the pinned-courses list that
        # is kept with many non-pin-related actions as context:
        self.pins_in_context_buf = BufferClass('pins_in_context_buf', self.DB_BATCH_SIZE)
        # Courses selected for deeper viewing
        self.crs_selects_buf = BufferClass('crs_selects_buf', int(self.DB_BATCH_SIZE / 2))
        # Buffer for search terms before going into db:
        self.crs_search_buf = BufferClass('crs_search_buf', int(self.DB_BATCH_SIZE / 2))
        
        # The long lists of enrollment history
        # that comes with some actions. Those
        # are NOT enrollments, which do not occur within
        # Carta:
        self.enrl_hist_buf = BufferClass('enrl_hist_buf', self.DB_BATCH_SIZE)
        
        # Set up map between each buffer and
        # the database table into which it empties.
        # The values contain the table name and the
        # names of the table's columns:
        
        self.buffer_tables = {
            self.activity_buf : ('Activities',
                                 (
                                    'row_id',
                                    'student',
                                    'ip_addr',
                                    'category',
                                    'action_nm' ,
                                    'created_at',
                                    'updated_at'
                                    )),
            self.pins_buf : ('Pins', ('row_id', 'crs_id')),
            self.unpins_buf : ('UnPins', ('row_id', 'crs_id')),
            self.pins_in_context_buf : ('ContextPins', ('row_id', 'crs_id')),
            self.crs_selects_buf : ('CrseSelects', ('row_id', 'crs_id')),
            self.crs_search_buf : ('CrseSearches', ('row_id', 'crs_id')),
            self.enrl_hist_buf : ('EnrollmentHist', ('row_id', 'crs_id'))
            }
        
        # Accumulator for search terms as they are typed
        # to avoid too many db entries of the same search
        # activity:
        self.search_term_accumulator = bytearray(50)
        # Currently searching emplid:
        self.crs_search_state = None
        
        self.log = LoggingService()
        
        if unittesting:
            return
        
        self.db = self.open_db()

        if self.is_gzipped(activity_log_path):
            open_func = gzip.open
        else:
            open_func = open
        
        with open_func(activity_log_path, 'rt') as fd:
            reader = csv.reader(fd, delimiter='\t')
            header = next(reader)
            for row in reader:
                try:
                    self.cur_id = int(row[ID_POS])
                except Exception as e:
                    self.log.err(f"Row does not have a row id: {row}")
                    continue
                self.dispatch_row(row, self.cur_id)

    #------------------------------------
    # process_one_row
    #-------------------
    
    def dispatch_row(self, row, row_id):
        
        caller = row[CALLER_POS]
        action = row[ACTION_POS]
        emplid = row[EMPLID_POS]
        
        # Check whether a search term is being typed in,
        # and the typing is done:
        
        if self.crs_search_state is not None:
            # Search done if either the cur activity
            # is by a different site visitor, or the
            # type of activity has changed away from
            # search. self.crs_search_state is
            #    (row_id, searching_emplid):
            if (self.crs_search_state[1] != emplid or \
                caller != 'find_search' or \
                action != 'search'):
                # Add the ongoing search action to the search buffer,
                # and then continue to process the row:
                self.commit_search_action()
            else:
                # Keep collecting search term chars:
                self.extract_find_search(row, row_id)
                return

        if caller == 'initial_recommendation':
            self.extract_pins_and_hist(row, row_id)
        elif caller == 'get_course_info':
            self.handle_select_course(row, row_id)
        elif caller == 'update_rec' and action in ('pin', 'unpin'):
            self.handle_pin_unpin(row, row_id)
        elif caller == 'find_search' and action == 'search':
            self.extract_find_search(row, row_id)
        else:
            print(caller)
            print('Foo')

    #------------------------------------
    # extract_pins
    #-------------------
    
    def extract_pins_and_hist(self, row, row_id):
        
        mv = memoryview(bytes(row[ENVIRONMENT_POS], 'utf8'))
        match = self.pins_and_enroll_hist_pat.search(mv)
        # Group 1 will be like: 
        #   '1156:208582, 1162:120904'
        for strm_crs_id_pair in match.group(1).split(b','):
            if len(strm_crs_id_pair) > 0:
                try:
                    strm, crs_id = strm_crs_id_pair.split(b':')
                except ValueError as e:
                    self.log.err(f"Could not split strm from crs_id in {match.group(1)}") 
                self.buffer(self.pins_in_context_buf, (row_id, int(strm), int(crs_id)))
    
        # Group 2 is a simple list of crs_id ints:
        if len(match.group(2)) > 0:
            self.buffer_int_arr(self.enrl_hist_buf, row_id, match.group(2))

    #------------------------------------
    # extract_course_select
    #-------------------
    
    def extract_course_select(self, row, row_id):
        
        # Get string: b'{selected_course:111846, name:BIO42}'
        sel_crs_id_match = self.crs_selection_pat.search(row[KEY_PARAMETER_POS])
        # Return the selected course:
        return int(sel_crs_id_match.group(1))

    #------------------------------------
    # extract_find_search
    #-------------------
    
    def extract_find_search(self, row, row_id):
        '''
        Process rows with CALLER 'find_search', and ACTION
        'search'. Several activity_log entries can result in
        a visitor typing their course query:
        Example:
                 cs 1
                 cs 10
                 cs 106a
        To thin these out we buffer the emerging string until
        any of the following occurs:
        
            o An activity_log entry from a different visitor
            o An activity_log entry the same visitor but a different
              CALLER/ACTION pair
                 
        :param row:
        :type row:
        :param row_id:
        :type row_id:
        '''

        # Get like: '{search_term_accumulator:cs 1}'
        search_snippet = row[KEY_PARAMETER_POS]
        search_term = self.search_term_pat.search(search_snippet).group(1)
        # Store the term as typed so far in the search term buffer:
        self.search_term_accumulator = search_term[0:]
        if self.crs_search_state is None:
            # First letter a searching visitor has typed:
            self.crs_search_state = (row_id, row[EMPLID_POS])
        
    #------------------------------------
    # commit_search_action
    #-------------------
    
    def commit_search_action(self):
        '''
        Searching for courses often triggers multiple
        activity_log actions as the search term is typed in.
        We accumulate the typed into until either no more
        activity records remain, or the end of the search text
        input is detected. In either of these cases, this 
        method is called.
        
        Expectation:
            o self.crs_search_state contains a 2-tuple whose
              fist entry is the activity log row number where
              the search started
            o self.search_term_accumulator contains the search term 

        the tuple:
        
             (row-id, search-term)
              
        is added to the crs_search_buf, from where it is eventually
        filled into the db.
        
        the crs_search_state is set to None to signal the end of
        one search activity.

        '''
        self.buffer(self.crs_search_buf, 
                    (self.crs_search_state[0], # row_id
                     self.search_term_accumulator
                     ))
        self.crs_search_state = None

    #------------------------------------
    # handle_select_course
    #-------------------
    
    def handle_select_course(self, row, row_id):
        '''
        A visitor clicked on a course search result.
        Extract the crs_id on which the visitor clicked.
        Add tuple (row_id, crs_id) to the crs_selects_buf
        buffer.
        
        :param row:
        :type row:
        :param row_id:
        :type row_id:
        '''

        crs_id = self.extract_course_select(row, row_id)
        self.buffer(self.crs_selects_buf, (row_id, crs_id))

    #------------------------------------
    # handle_pin_unpin
    #-------------------
    
    def handle_pin_unpin(self, row, row_id):
        '''
        Called when CALLER is update_rec, and ACTION
        is either pin or unpin. Add the actions to
        the pin_buf or unpin_buf.
        
        :param row:
        :type row:
        :param row_id:
        :type row_id:
        '''

        crs_id = self.extract_course_select(row, row_id)
        if row[ACTION_POS] == 'pin':
            self.buffer(self.pins_buf, (row_id, crs_id))
        else:
            self.buffer(self.unpins_buf, (row_id, crs_id))

# --------------------- Utilities ------------

    #------------------------------------
    # buffer
    #-------------------
    
    def buffer(self, buf, content):
        '''
        Append content to buffer. When buffer is
        full as per DB_BATCH_SIZE, all buffers are
        written to the database, and are emptied.
        
        Example content:
        
           [10, 1102, 123456]
           
        where 10 is a row ID, 1102 is a strm, and 123456 is
        a course ID.
        
        This tuple is appended to the buffer.
        
        :param buf:
        :type buf:
        :param content:
        :type content:
        '''
        
        buf.append(content)
        if len(buf) >= self.DB_BATCH_SIZE:
            self.flush_buffer(buf)

    #------------------------------------
    # buffer_simple_arr
    #-------------------
    
    def buffer_int_arr(self, buf, row_id, simple_arr):
        '''
        Extend content to buffer. I.e. this method
        assumes the buffer is a an array that keeps
        getting longer. Each element of simple_arr
        is added as a pair with the current row id.
         
        When buffer is full as per DB_BATCH_SIZE, all 
        buffers are written to the database, and are emptied.
        
        :param buf:
        :type buf:
        :param row_id,
        :type row_id,
        :param simple_arr:
        :type simple_arr:
        '''

        # For each element of simple_arr, create a tuple
        # [row_id, arrayElement], and append that to the
        # buffer:
        
        int_len = 0
        for asc_val in simple_arr[1:]:
            if asc_val in range(ord('0'), ord('9')+1):
                int_len += 1
            else:
                break
            
        inter_int_width = 0
        for asc_val in simple_arr[int_len+1:]:
            if asc_val not in range(ord('0'), ord('9')+1):
                inter_int_width += 1
            else:
                break

        int_arr = []
        for int_start in range(1, len(simple_arr) , int_len+inter_int_width):
            try:
                int_arr.append(int(simple_arr[int_start:int_start+int_len]))
            except ValueError:
                # Likely end of numbers
                break

        if len(int_arr) > 0:
            buf.extend(list(map(lambda arr_el: (row_id, arr_el), int_arr)))
        if len(buf) >= self.DB_BATCH_SIZE:
            self.flush_buffer(buf)

    #------------------------------------
    # open_db
    #-------------------

    def open_db(self):
        #print("SkIPPING DB")
        #return
        try:
            pwd_file = os.path.join(os.getenv('HOME'), '.ssh/mysql')
            with open(pwd_file, 'r') as fd:
                pwd = fd.read()
        except Exception as e:
            raise PermissionError(f"Cannot read MySQL pwd from {pwd_file}: {repr(e)}")

        uname = getpass.getuser()

        try:
            db = MySQLDB(user=uname, passwd=pwd, db=self.DB_NAME) 
        except Exception as e:
            raise RuntimeError(f"Cannot access db for user {uname} db {self.DB_NAME}: {repr(e)}")

        # Test whether all necessary tables exist:
        for tbl_nm, cols in self.buffer_tables:
            res = self.db.query(f'''SELECT COUNT(*)
                                    FROM information_schema.tables 
                                    WHERE table_schema = "{self.DB_NAME}"
                                    AND table_name = "{tbl_nm}";'''
            )
            if next(res) == 0:
                self.create_tbl(tbl_nm, cols)

        return db

    #------------------------------------
    # create_tbl
    #-------------------
    
    def create_tbl(self, tbl_nm):

        if tbl_nm in ['Pins', 'UnPins', 'CrseSelects', 'EnrollmentHist']:
            self.db.createTable(tbl_nm, {'row_id': 'int',
                                         'crs_id' : 'int'
                                         })
        elif tbl_nm == 'ContextPins':
            self.db.createTable(tbl_nm, {'row_id': 'int',
                                         'quarter_id' : 'int',
                                         'crs_id' : 'int'
                                         })
        elif tbl_nm == 'CrseSearches':
            self.db.createTable(tbl_nm, {'row_id': 'int',
                                         'search_term' : 'varchar(255)'
                                         })

        elif tbl_nm == 'Activities':
            self.db.createTable(tbl_nm, {'row_id'    : 'int',
                                         'student'   : 'varchar(100)',
                                         'ip_addr'   : 'varchar(16)',
                                         'category'  : 'varchar(30)',
                                         'action_nm' : 'varchar(30)',
                                         'created_at': 'datetime',
                                         'updated_at': 'datetime'
                                         })

    #------------------------------------
    # flush_buffer
    #-------------------
    
    def flush_buffer(self, buf):
        '''
        Empty the given buffer into its appropriate
        database table, and truncate the buffer.
        
        We handle all buffers that are mapped to db
        tables in self.buffer_tables. The data in the buffers
        is structured like this:
        
            pins_in_context: (row_id, strm, crs_id)
            crs_search    : (row_id, search_term)
        all others:
                           : (row_id, crs_id)
        
        :param buf:
        :type buf:
        '''

        dest_tbl, col_names = self.buffer_tables[buf]
        # Try to write, create the table if needed:
        (errs, warns) = self.db.batch_insert(dest_tbl, col_names, buf)
        

    #------------------------------------
    # is_gzipped
    #-------------------
    
    def is_gzipped(self, path):
        '''
        Returns True if file at path is gzipped,
        else False. Throws FileNotFoundError if file
        does not exist
        
        :param path: path to file
        :type path: str
        :return whether or not file is gzipped
        :rtype bool
        :raise FileNotFoundError if file does not exist
        '''
        
        with gzip.open(path) as fd:
            try:
                fd.read(1)
                return True
            except (gzip.BadGzipFile, OSError):
                return False

# ------------------------- BufferClass ----------------

class BufferClass:
    
    #------------------------------------
    # Constructor 
    #-------------------
    
    def __init__(self, name, capacity):
        self.name = name
        self.capacity = capacity
        self.arr = []

    #------------------------------------
    # append
    #-------------------
    
    def append(self, elements):
        self.arr.append(elements)
        
    #------------------------------------
    # extend
    #-------------------
    
    def extend(self, elements):
        self.arr.extend(elements)



    #------------------------------------
    # full
    #-------------------
    
    def full(self):
        '''
        True if buffer has as many elements as were
        specified for its capacity upon creation.
        :param self:
        :type self:
        '''

        return len(self.arr) >= self.capacity

    #------------------------------------
    # __hash__
    #-------------------
    
    def __hash__(self):
        return id(self)

    #------------------------------------
    # __repr__
    #-------------------
    
    def __repr__(self):
        return f"<DbBuf {self.name} {hex(id(self))}>"

    #------------------------------------
    # __str__
    #-------------------
    
    def __str__(self):
        return self.__repr__()
        


# ------------------------ Main ------------
if __name__ == '__main__':
    
    #ActivityLogCleaner('/Users/paepcke/Project/Carta/Data/CartaData/ActivityLog/activity_logDec21_2018.csv')
    #ActivityLogCleaner('/tmp/activity_log_2015_Oct24_to_2021_Nov19.tsv')
    #ActivityLogCleaner('/tmp/activity_log_two_lines_clean.tsv')
    #ActivityLogCleaner('/tmp/activity_log_two_lines_cleanest.tsv')
    ActivityLogCleaner('/tmp/activity_log_2015_Oct24_to_2021_Nov19_Cleanest.tsv.gz')
