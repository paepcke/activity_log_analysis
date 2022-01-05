#!/usr/bin/env python
'''
Created on Nov 18, 2021

@author: paepcke
'''
import argparse
import csv
import getpass
import gzip
import os
import re
import sys
import time

from logging_service import LoggingService
from pymysql_utils.pymysql_utils import MySQLDB

from actlog.ipToFullLocation import IpFullLocation


# For running in Eclipse on Mac: add path to mysql client:
#**********
os.environ['PATH'] = '/usr/local/bin/:' + os.environ['PATH']
#**********

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

#****** WRITE DROP TABLES

class ActivityLogCleaner(object):
    '''
    classdocs
    '''

    DB_BATCH_SIZE_BIG = 20000
    DB_BATCH_SIZE_SMALL = 1000
    DB_NAME = 'activity_log'
    MAX_SEARCH_TERM_LEN = 2000
    
    DEFAULT_IPLOC_TUPLE = ('--',
                           'Country-Unknown',   # Country  
                           'State-Unknown',     # State/Province
                           'City-Unknown',      # City
                           '0.0',               # Lat
                           '0.0',               # Long
                           'Zip-Unknown',       # Zip/Postal code
                           'TZ-Unknown',        # Time zone
                           '-1'                 # Phone country number
                           '-1'                 # Area code
                           )
    '''Used when IP Address not in the database'''

    SECS_BETWEEN_HEARTBEATS = 5

    STRM_LEN = 4

    caller_pat = re.compile(r"")

    # For extracting pins from ENVIRONMENT context
    # for the two varieties: early format and late
    # format (2016-2017, 2017-present):
    
    # If successful, the following pattern returns
    #    b'1156:208582, 1162:120904'
    context_pinned_early_var_pat = re.compile(b'.*pinned:{([^}]*)}.*')
    
    # If successful, the following pattern returns
    # the list shown in header comment:
    context_pinned_late_var_phase1_pat = re.compile(b'.*pinned_courses"=>\[([^]]*)].*')
    # Pick the crse_id nums from the pin specs 
    # Intended to be used with pattern.findall().
    # Returns:
    #   [(b'1214', b'204608'), (b'1214', b'105670'), (b'1214', b'105687'), (b'1214', b'219885')]
    context_pinned_late_var_phase2_pat = re.compile(b'STRM: ([0-9]{4}), CLASS_NBR: [^,]*, CRSE_ID: ([0-9]{6})')
    
    context_enrl_hist_early_pat = re.compile(b'course_history_ids:([^\]]*])')
    
    # Pick up the enrire list of #<Enrollment STRM:nil, ... CRSE_ID: 156872, ...>, #<Enrollment...>]
    context_enrl_hist_late_phase1_pat  = re.compile(b'registered_courses"=>([^\]]*])')
    # Pick STRM and CRS_ID nums out of the #<Enrollment STRM:nil, ... CRSE_ID: 156872, ...> entries.
    # Intended to be used with findall().
    context_enrl_hist_late_phase2_pat  = re.compile(b'CRSE_ID: ([0-9]{6})') 
    
    p_history_pat = re.compile(b"#<Enrollment (STRM: [0-9]{4}, CRSE_ID: [0-9]{6})")
    # Extract 'selected course' id from 
    #   {selected_course:111846, name:BIO42}
    crs_selection_pat = re.compile(r'[^:]*:([^,]*).*')
    
    # Extract instructor sunet id from
    #   '{sunet:rjohari}'
    instructor_profile_pat = re.compile(r'[^:]*:([^}]*).*')
    
    # Search term pattern: extract search term from
    #    '{search_term_accumulator:cs 1}
    # i.e. extract 'cs 1':
    search_term_pat = re.compile(r'[^:]*:([^}]*)')
    
    #------------------------------------
    # Constructor 
    #-------------------

    def __init__(self, 
                 activity_log_path,
                 db_user=None,
                 db_name=None,
                 db_pwd=None,        # ******Remove 
                 start_fresh = False,
                 unittesting=False):
        '''
        Constructor
        '''
        self.log = LoggingService()

        self.db_user = db_user
        
        # The data for each action type is accumulated
        # in a separate buffer, and pushed down into a
        # corresponding table when the buffer is full. 
        # The following are the buffers:
        
        # Holds one tuple for each action:
        self.activity_buf = BufferClass('activity_buf', self.DB_BATCH_SIZE_BIG)
         
        # Three buffers around pinning:
        # two for the action of pinning and unpinning...:
        self.pins_buf = BufferClass('pins_buf', self.DB_BATCH_SIZE_SMALL)
        self.unpins_buf = BufferClass('unpins_buf', self.DB_BATCH_SIZE_SMALL)
        # ... the third for the pinned-courses list that
        # is kept with many non-pin-related actions as context:
        self.pins_in_context_buf = BufferClass('pins_in_context_buf', self.DB_BATCH_SIZE_SMALL)
        # Courses selected for deeper viewing
        self.crs_selects_buf = BufferClass('crs_selects_buf', self.DB_BATCH_SIZE_BIG)
        # Buffer for search terms before going into db:
        self.crs_search_buf = BufferClass('crs_search_buf', self.DB_BATCH_SIZE_SMALL)
        
        # The long lists of enrollment history
        # that comes with some actions. Those
        # are NOT enrollments, which do not occur within
        # Carta:
        self.enrl_hist_buf = BufferClass('enrl_hist_buf', self.DB_BATCH_SIZE_BIG)

        # Visitors lookup up a particular instructor:
        self.instructor_lookup_buf = BufferClass('instructor_lookup_buf', self.DB_BATCH_SIZE_SMALL)
        
        # IP Address reference:
        self.ip_location_buf = BufferClass('ip_location_lookup_buf', self.DB_BATCH_SIZE_BIG)
        
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
            self.pins_in_context_buf : ('ContextPins', ('row_id', 'quarter_id', 'crs_id')),
            self.crs_selects_buf : ('CrseSelects', ('row_id', 'crs_id')),
            self.crs_search_buf : ('CrseSearches', ('row_id', 'search_term')),
            self.enrl_hist_buf : ('EnrollmentHist', ('row_id', 'crs_id')),
            self.instructor_lookup_buf : ('InstructorLookups', ('row_id', 'instructor')),
            self.ip_location_buf : ('IpLocation', 
                                    (
                                    'row_id',
                                    'country_code',
                                    'country',
                                    'state',
                                    'city',
                                    'lat',
                                    'longitude',
                                    'zip',
                                    'time_zone',
                                    'country_phone',
                                    'area_code'))
            }
        
        # Accumulator for search terms as they are typed
        # to avoid too many db entries of the same search
        # activity:
        self.search_term_accumulator = bytearray(50)
        # Currently searching emplid:
        self.crs_search_state = None
        
        if unittesting:
            return
        
        self.db = self.open_db(uname=self.db_user, pwd=db_pwd, start_fresh=start_fresh)

        self.log.info("Loading IP locations...")
        self.ip_dict = IpFullLocation()
        self.log.info("Done loading IP locations.")

        if self.is_gzipped(activity_log_path):
            open_func = gzip.open
        else:
            open_func = open
        
        # For timing printouts of number of records processed:
        prev_sign_of_life = int(time.time())
        
        # Open the activity log CSV or CSV.ZIP:
        with open_func(activity_log_path, 'rt') as fd:
            # Process a row at a time:
            reader = csv.reader(fd, delimiter='\t')
            _header = next(reader)
            for row in reader:
                try:
                    self.cur_id = int(row[ID_POS])
                except Exception as _e:
                    self.log.err(f"Row does not have a row id: {row}")
                    continue
                self.dispatch_row(row, self.cur_id)
                cur_time = int(time.time())
                if (cur_time - prev_sign_of_life) > self.SECS_BETWEEN_HEARTBEATS:
                    print(f"At record {self.cur_id}", end='\r')
                    prev_sign_of_life = cur_time
            # Finished:
            for buf in self.buffer_tables.keys():
                self.flush_buffer(buf)
            self.db.close()
            # Break out of the inline progress report:
            print()
            self.log.info(f"Imported {self.cur_id} records.")

        # Create indexing:
        self.log.info("Creating indexes on row_id for ...")
        self.log.info("Activities ...")
        self.db.execute("CREATE UNIQUE INDEX row_id_idx ON Activities(row_id)")
        self.log.info("ContextPins ...")
        self.db.execute("CREATE INDEX row_id_idx ON ContextPins(row_id);")
        self.log.info("CrseSearches ...")
        self.db.execute("CREATE INDEX row_id_idx ON CrseSearches(row_id);")
        self.log.info("CrseSelects ...")
        self.db.execute("CREATE INDEX row_id_idx ON CrseSelects(row_id);")
        self.log.info("EnrollmentHist ...")
        self.db.execute("CREATE INDEX row_id_idx ON EnrollmentHist(row_id);")
        self.log.info("InstructorLookups ...")
        self.db.execute("CREATE INDEX row_id_idx ON InstructorLookups(row_id);")
        self.log.info("Pins ...")
        self.db.execute("CREATE INDEX row_id_idx ON Pins(row_id);")
        self.log.info("UnPins ...")
        self.db.execute("CREATE INDEX row_id_idx ON UnPins(row_id);")
        self.log.info("IpLocation ...")
        self.db.execute("CREATE INDEX row_id_idx ON IpLocation(row_id);")
        
        self.log.info("Creating indexes on crs_id for ...")
        self.log.info("ContextPins ...")
        self.db.execute("CREATE INDEX crs_id_idx ON ContextPins(crs_id);")
        self.log.info("CourseInfo ...")
        self.db.execute("CREATE INDEX crs_id_idx ON CourseInfo(crs_id);")
        self.log.info("CrseSelects ...")
        self.db.execute("CREATE INDEX crs_id_idx ON CrseSelects(crs_id);")
        self.log.info("EnrollmentHist ...")
        self.db.execute("CREATE INDEX crs_id_idx ON EnrollmentHist(crs_id);")
        self.log.info("Pins ...")
        self.db.execute("CREATE INDEX crs_id_idx ON Pins(crs_id);")
        self.log.info("UnPins ...")
        self.db.execute("CREATE INDEX crs_id_idx ON UnPins(crs_id);")
        
        self.log.info("Creating indexes on subject for ...")
        self.log.info("SubjSchoolSubschoolDep ...")
        self.db.execute("CREATE INDEX subj_idx ON SubjSchoolSubschoolDep(subject);")
        self.log.info("CourseInfo ...")
        self.db.execute("CREATE INDEX subj_idx ON CourseInfo(subject);")
        
        self.log.info("Creating index on created_at for Activities...")
        self.db.execute("CREATE INDEX created_at_idx ON Activities(created_at);")
        self.log.infor("Done indexing")


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
            # search. self.crs_search_state is a dict:
            #
            #     {'row_id' : row_id, 
            #      'emplid' : row[EMPLID_POS],
            #      'ip_address' : row[IP_ADDRESS_POS],
            #      'caller' : row[CALLER_POS],
            #      'action' : row[ACTION_POS]
            #      'created_at' : row[CREATED_AT_POS],
            #      'updated_at' : row[UPDATED_AT_POS]
            #      }            
            
            if (self.crs_search_state['emplid'] != emplid or \
                caller != 'find_search' or \
                action != 'search'):
                # Add the ongoing search action to the search buffer,
                # and then continue to process the row:
                self.commit_search_action()
            else:
                # Keep collecting search term chars:
                self.extract_find_search(row, row_id)
                return

        else:
            # Not in middle of search word typing:
            self.add_activity_record(row)

        # Add other info contained in the row:

        if caller == 'initial_recommendation':
            self.extract_pins(row, row_id)
            self.extract_enrl_history(row, row_id)
        elif caller == 'get_course_info':
            self.handle_select_course(row, row_id)
        elif caller in ['update_rec', 'pin', 'unpin'] and action in ('pin', 'unpin'):
            self.handle_pin_unpin(row, row_id)
        elif caller in ['find_search', 'detailed_search'] and action in ['search', 'search_query']:
            self.extract_find_search(row, row_id)
        elif caller == 'instructor_profile' and action == 'instructor':
            self.extract_instructor_profile(row, row_id)
        else:
            if caller in ['get_recommendations', 'pair', 'unpair', 'join_carta',
                          'post_feedback'
                          ] or \
                action in ['discount', 'undiscount', 'show_landing_page',
                           'show_index_page', 'store_calendar_state',
                           'user_message', 'confirm_user_message',
                           'reset_confirm_user_message', 'welcome_to_carta',
                           'repin', 'decline_user_message', 'reset_lcs_response',
                           'join_carta_interview']:
                return
            else:
                print(f"Unimplemented activity: {caller}/{action}")


    #------------------------------------
    # add_activity_record
    #-------------------
    
    def add_activity_record(self, row):
        '''
        Add the main activity record, given either
        a row as a list from the csv read, or a 
        with the values:
            row[ID_POS], 
            row[EMPLID_POS],
            row[IP_ADDRESS_POS],
            row[CALLER_POS],
            row[ACTION_POS]
        
        :param row:
        :type row
        '''
        row_id = row[ID_POS]
        if type(row) == list:
            activity_tuple = (
                row_id,
                row[EMPLID_POS],
                row[IP_ADDRESS_POS],
                row[CALLER_POS],
                row[ACTION_POS],
                row[-2],
                row[-1]
                )
        else:
            activity_tuple = row
            
        self.buffer(self.activity_buf, activity_tuple)
        # Fill a row in the IpLocation table
        ip_loc_tuple = self.ip_dict.get(row[IP_ADDRESS_POS], 
                                        default=self.DEFAULT_IPLOC_TUPLE)
        self.buffer(self.ip_location_buf, (row_id,) + ip_loc_tuple)

    #------------------------------------
    # extract_pins
    #-------------------
    
    def extract_pins(self, row, row_id):
        '''
        Extract the 'currently-pinned' strm and crs_id nums.
        Over time the format changed. There are two varieties:
        
        Early variety:
            ...\t...\t{pinned:{1156:208582, 1162:120904},...\t 
        
        Later variety:
            ...\t"pinned_courses"=>[#<Enrollment STRM: 1214, CLASS_NBR: 25600, CRSE_ID: 204608, 
                                       CATALOG_NBR: "151", SUBJECT: "ARCHLGY", 
                                       DESCRIPTION: "Ten Things: An Archaeology of Design ...">, 
                                    #<Enrollment STRM: 1214, CLASS_NBR: 18259, CRSE_ID: 105670, 
                                       CATALOG_NBR: "140", SUBJECT: "CS", 
                                       DESCRIPTION: "Operating Systems and Systems Programming">,
                                       ...
                                    ] 
        we try both for each row (sigh...)
        
        For the later variety we go in two steps: get
        the above-shown list out of the row. Then pick
        out STRM and CRSE_ID. 
        
        :param row:
        :type row:
        :param row_id:
        :type row_id:
        '''

        # Avoid creating new strings from the row
        mv = memoryview(bytes(row[ENVIRONMENT_POS], 'utf8'))
        
        match_early_var = self.context_pinned_early_var_pat.search(mv)
        if match_early_var is not None:
            self._clean_pins(row_id, match_early_var, 'early_var_res')
            return
        # Try the late variety:
        match_late_var = self.context_pinned_late_var_phase1_pat.search(mv)
        if match_late_var is not None:
            self._clean_pins(row_id, match_late_var, 'late_var_res')
        return

    #------------------------------------
    # _clean_pins
    #-------------------

    def _clean_pins(self, row_id, match_obj, match_type):
        '''
        Called when pins were provided as part of an activity. 
        The match type is either 'early_var_res' or 'late_var_res'.
        Early varieties look like:
        
            b'1156:208582, 1162:120904, ...'
        
        while late varieties look like:
        
            [#<Enrollment STRM: 1214, CLASS_NBR: 25600, CRSE_ID: 204608, 
                CATALOG_NBR: "151", SUBJECT: "ARCHLGY", 
                DESCRIPTION: "Ten Things: An Archaeology of Design ...">, 
             #<Enrollment STRM: 1214, CLASS_NBR: 18259, CRSE_ID: 105670, 
                CATALOG_NBR: "140", SUBJECT: "CS", 
                DESCRIPTION: "Operating Systems and Systems Programming">,
                ...
             ]
        
        Either way, we create a triplet for each pin:
        
               (row_id, strm, crs_id)

        The match object is known to be non-None, and group(1) will
        contain the finding:
        
        :param row_id:
        :type row_id:
        :param match_obj:
        :type match_obj:
        :param match_type:
        :type match_type:
        '''
        if match_type == 'early_var_res':
            for strm_crs_id_pair in match_obj.group(1).split(b','):
                if len(strm_crs_id_pair) > 0:
                    try:
                        strm, crs_id = strm_crs_id_pair.split(b':')
                    except ValueError as _e:
                        self.log.err(f"Could not split strm from crs_id in {match_obj.group(1)}") 
                    self.buffer(self.pins_in_context_buf, (row_id, int(strm), int(crs_id)))
            return
        else:
            # Must proceed in two steps. First, pick out
            # all the STRM and CRSE_ID number pairs:

            # Get: 
            #   [(b'1214', b'204608'), (b'1214', b'105670'), (b'1214', b'105687'), ...]
            res_list = self.context_pinned_late_var_phase2_pat.findall(match_obj.group(1))
            if res_list is None:
                return
            # Create the triplets, and buffer them:
            for strm, crs_id in res_list:
                self.buffer(self.pins_in_context_buf, (row_id, int(strm), int(crs_id)))

    #------------------------------------
    # extract_enrl_history
    #-------------------
    
    def extract_enrl_history(self, row, row_id):
        
        mv = memoryview(bytes(row[ENVIRONMENT_POS], 'utf8'))
        # Try the early-version log format:
        
        crs_id_match = self.context_enrl_hist_early_pat.search(mv)
        if crs_id_match is not None:
            self.buffer_int_arr(self.enrl_hist_buf, row_id, crs_id_match.group(1))
            
        else:
            # Try later form:
            crs_id_envs_match = self.context_enrl_hist_late_phase1_pat.search(mv)
            if crs_id_envs_match is None:
                return
            # Pick the CRSE_ID nums out of the environment objs.
            # Result will be [b'123456', b'789012, ...]
            crse_id_arr_of_bstrings = self.context_enrl_hist_late_phase2_pat.findall(crs_id_envs_match.group(1))
            int_arr = [int(el) for el in crse_id_arr_of_bstrings]
            self.buffer_int_arr(self.enrl_hist_buf, row_id, int_arr)


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
            # First letter a searching visitor has typed. 
            # Save the main activity info, it will be used
            # for the db record entry when typing has ended.
            
            # But first: visitors sometimes enter a tab in the
            # search field, which confuses csv. Result is a row like:
            #     ['37153',
            #      '$2b$15$EbB/W969b4O21hXhf.5C7O4AjMc1y9.pkR4fguwNkVc41bPIrbXjC',
            #      '10.30.49.129',
            #      'find_search',
            #      'search',
            #      '{search_term:lawgen', '}', 'NULL', '{results:[212339]}',...]
            #   --------------------------^^^----------
            # Remove the extra element:
            
            self.crs_search_state = {'row_id' : int(row_id), 
                                     'emplid' : row[EMPLID_POS],
                                     'ip_address' : row[IP_ADDRESS_POS],
                                     'caller' : row[CALLER_POS],
                                     'action' : row[ACTION_POS],
                                     'created_at' : row[-2],
                                     'updated_at' : row[-1]
                                     }

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
            o self.crs_search_state contains a a dict
                 {'row_id' : row_id, 
                  'emplid' : row[EMPLID_POS],
                  'ip_address' : row[IP_ADDRESS_POS],
                  'caller' : row[CALLER_POS],
                  'action' : row[ACTION_POS],
                  'created_at' : row[CREATED_AT_POS],
                  'updated_at' : row[UPDATED_AT_POS]
                  }            

            o self.search_term_accumulator contains the search term 

        the tuple:
        
             (row-id, search-term)
              
        is added to the crs_search_buf, from where it is eventually
        filled into the db.
        
        the crs_search_state is set to None to signal the end of
        one search activity.

        '''
        self.buffer(self.crs_search_buf, 
                    (self.crs_search_state['row_id'],
                     self.search_term_accumulator
                     ))
        # Add the activity record for this now concluded
        # search:
        self.add_activity_record(tuple(self.crs_search_state.values()))
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
    # extract_instructor_profile
    #-------------------

    def extract_instructor_profile(self, row, row_id):
        
        instructor_spec = row[KEY_PARAMETER_POS]
        instructor = self.instructor_profile_pat.search(instructor_spec).group(1)
        self.buffer(self.instructor_lookup_buf, (row_id, instructor))

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
        full as per DB_BATCH_SIZE_BIG, all buffers are
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
        if buf.full():
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
         
        When buffer is full as per DB_BATCH_SIZE_BIG, all 
        buffers are written to the database, and are emptied.
        
        Example for simple_arr:
           b'[102794, 105644, 105645, 105649]'
        or:
           [102794, 105644, 105645, 105649]  
        
        :param buf:
        :type buf:
        :param row_id,
        :type row_id,
        :param simple_arr: bin string containing a list of ints,
            or a list of ints
        :type simple_arr: {b[int] | [int]}
        '''

        # Turn the bin str into an array of int.
        #    b'[102794, 105644, 105645, 105649]'
        # => [102794, 105644, 105645, 105649]
        if type(simple_arr) == bytes:
            int_arr = eval(simple_arr)
        else:
            int_arr = simple_arr

        # For each element of simple_arr, create a tuple
        # [row_id, arrayElement], and append that to the
        # buffer:

        if len(int_arr) > 0:
            buf.extend(list(map(lambda arr_el: (row_id, arr_el), int_arr)))
        if buf.full():
            self.flush_buffer(buf)

    #------------------------------------
    # open_db
    #-------------------

    def open_db(self, uname=None, db_name=None, pwd=None, start_fresh=False):
        #print("SkIPPING DB")
        #return
        if pwd is None:
            try:
                pwd_file = os.path.join(os.getenv('HOME'), '.ssh/mysql')
                with open(pwd_file, 'r') as fd:
                    pwd = fd.read().strip()
            except Exception as e:
                raise PermissionError(f"Cannot read MySQL pwd from {pwd_file}: {repr(e)}")

        if uname is None:
            uname = getpass.getuser()
        if db_name is None:
            db_name = self.DB_NAME

        try:
            db = MySQLDB(user=uname, passwd=pwd, db=db_name) 
        except Exception as e:
            raise RuntimeError(f"Cannot access db for user {uname} db {self.DB_NAME}: {repr(e)}")

        self.db = db

        # Check whether Activities table exists, and 
        # warn about wiping out all tables:
        res_iter = db.query('''SELECT COUNT(*)
                          FROM information_schema.tables 
                          WHERE table_schema = DATABASE()
                          AND table_name = "Activities";
                          )
                          ''')
        activity_tbl_exists = next(res_iter)
        try:
            # Terminate the query iterator
            # WTF is going on? Should not be needed,
            # but otherwise bogus MySQL programming error
            # on next query:
            next(res_iter)
        except Exception:
            pass
            
        if activity_tbl_exists:
            response = input("Tables already exist, wipe them? (y/n): ")
            if response in ('y', 'Y'):
                start_fresh = True
            else:
                start_fresh = False

        # Test whether all necessary tables exist:
        for tbl_nm, _cols in self.buffer_tables.values():
            res = next(db.query(f'''SELECT COUNT(*)
                                FROM information_schema.tables 
                               WHERE table_schema = "{self.DB_NAME}"
                                 AND table_name = "{tbl_nm}";'''
            ))
            if res == 0:
                self.create_tbl(tbl_nm)

            # Truncate table if starting over:
            if start_fresh:
                db.truncateTable(tbl_nm)

        # No truncated, overlong search terms yet:
        self.truncated_search_terms = 0

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
                                         'search_term' : f'varchar({self.MAX_SEARCH_TERM_LEN})'
                                         })

        elif tbl_nm == 'Activities':
            self.db.execute('''CREATE TABLE Activities (
                            row_id    : int NOT NULL,
                            student   : varchar(100),
                            ip_addr   : varchar(16),
                            category  : varchar(30),
                            action_nm : varchar(30),
                            created_at: datetime,
                            updated_at: datetime,
                            PRIMARY KEY(row_id)
                            ) engine=MyISAM
                            '''
            )
            self.db.createTable(tbl_nm, {'row_id'    : 'int',
                                         'student'   : 'varchar(100)',
                                         'ip_addr'   : 'varchar(16)',
                                         'category'  : 'varchar(30)',
                                         'action_nm' : 'varchar(30)',
                                         'created_at': 'datetime',
                                         'updated_at': 'datetime'       
                                         })
            
        elif tbl_nm == 'InstructorLookups':
            self.db.createTable(tbl_nm, {'row_id': 'int',
                                         'instructor' : 'varchar(40)'
                                         })
            
        elif tbl_nm == 'IpLocation':
            self.db.createTable(tbl_nm, {'row_id' : 'int',
                                         'country_code' : 'varchar(2)',
                                         'country' : 'varchar(60)',
                                         'state' : 'varchar(100)',
                                         'city' : 'varchar(100)',
                                         'lat' : 'varchar(40)',
                                         'longitude' : 'varchar(40)',
                                         'zip' : 'varchar(20)', 
                                         'time_zone' : 'varchar(10)',
                                         'country_phone' : 'varchar(5)', 
                                         'area_code' : 'varchar(40)'})
        # I prefer MyISAM engine:
        self.db.execute(f"ALTER TABLE {tbl_nm} engine=MyISAM;")

    #------------------------------------
    # drop_tables
    #-------------------
    
    def drop_tables(self):
        '''
        Drop all tables that get created 
        from the activity log. 
        '''

        for tbl_nm, _cols in self.buffer_tables.values():
            query = f'''SELECT COUNT(*)
                          FROM information_schema.tables 
                         WHERE table_schema = "{self.DB_NAME}"
                           AND table_name = "{tbl_nm}";'''
            tbl_exists = next(self.db.query(query))
            if tbl_exists == 1:
                self.db.dropTable(tbl_nm)
                
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
        (errs, warns) = self.db.bulkInsert(dest_tbl, col_names, buf)
        
        if errs is not None:
            self.log.err(f"Errors insert into tbl {dest_tbl}: {errs}")
        if warns is not None:
            # Filter out the overlong search terms:
            for warn in warns:
                if warn[2].find("Data truncated for column 'search_term'") > -1:
                    self.truncated_search_terms += 1
                    print(f"Search terms truncated: {self.truncated_search_terms}")
                else:
                    self.log.warn(f"Warnings insert into tbl {dest_tbl}: {warns}")
                #**************
                #row_num = re.search(r'at row ([0-9]*).*', 'foo at row 878 and').group(1)
                #print('foo')
                #**************

            self.log.warn(f"Warnings insert into tbl {dest_tbl}: {warns}")

        buf.truncate()

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
    # truncate
    #-------------------
    
    def truncate(self):
        self.arr = []

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
    # __iter__
    #-------------------
    
    def __iter__(self):
        return iter(self.arr)


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
    
    parser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]),
                                     formatter_class=argparse.RawTextHelpFormatter,
                                     description="Parse activity log tsv file"
                                     )

    parser.add_argument('-p', '--password',
                        action='store_true',
                        help='whether or not to prompt for databases pwd; default: content of file ~/.ssh/mysql',
                        default=False
                        )
    
    # parser.add_argument('-s' '--startFresh',
    #                     action='store_true',
    #                     default=False,
    #                     help='whether or not to clear all tables first')
    
    parser.add_argument('-u', '--user',
                        type=str,
                        help=f'databases user; default {getpass.getuser()}')

    parser.add_argument('activity_log_path',
                        type=str,
                        help='Path to activity tsv file; may be gzipped or unzipped')


    args = parser.parse_args()

    if not os.path.exists(args.activity_log_path):
        print(f"Cannot find file {args.activity_log_path}")
        sys.exit(1)

    if args.user is None:
        user = getpass.getuser()
    else:
        user = args.user

    if args.password:
        pwd = getpass.getpass(prompt=f"Database password for {user}")
    else:
        pwd = None
    
    ActivityLogCleaner(args.activity_log_path,
                       db_user=user,
                       db_pwd=pwd,
                       start_fresh=True
                       )
    
    #ActivityLogCleaner('/Users/paepcke/Project/Carta/Data/CartaData/ActivityLog/activity_logDec21_2018.csv')
    #ActivityLogCleaner('/tmp/activity_log_2015_Oct24_to_2021_Nov19.tsv')
    #ActivityLogCleaner('/tmp/activity_log_two_lines_clean.tsv')
    #ActivityLogCleaner('/tmp/activity_log_two_lines_cleanest.tsv')
    #ActivityLogCleaner('/tmp/activity_log_2015_Oct24_to_2021_Nov19_Cleanest.tsv.gz',
    #                   db_user='root',
    #                   db_pwd='',  # ******Remove
    #                   start_fresh=True
    #                   )
