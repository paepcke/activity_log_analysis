'''
Created on Nov 22, 2021

@author: paepcke
'''
import os
import unittest

from actlog.activity_log_cleaning import ActivityLogCleaner, BufferClass
from actlog.ipToFullLocation import IpFullLocation

#*****TEST_ALL = True
TEST_ALL = False


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cur_dir = os.path.dirname(__file__)

# ---------------------- Tests --------------

    #------------------------------------
    # test_init_db
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_init_db(self):
        
        actlog_cleaner = ActivityLogCleaner(None, unittesting=True)
        actlog_cleaner.open_db(uname='root', db_name='activity_log', pwd='')

    #------------------------------------
    # test_buffer_int_array
    #-------------------

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_buffer_int_array(self):
        
        str_arr = b'[123, 456]'
        res_buf = BufferClass('test_buf', 100)
        expected = [(1,123), (1, 456)]
        
        actlog_cleaner = ActivityLogCleaner(None, unittesting=True)
        actlog_cleaner.buffer_int_arr(res_buf, 1, str_arr)
        
        self.assertListEqual(res_buf.arr, expected)
        
        # No space after comma:
        str_arr = b'[123,456]'
        res_buf = BufferClass('test_buf', 100)
        actlog_cleaner.buffer_int_arr(res_buf, 1, str_arr)
        
        self.assertListEqual(res_buf.arr, expected)

    #------------------------------------
    # test_extract_course_selects
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_extract_course_selects(self):
        
        actlog_cleaner = ActivityLogCleaner(None, unittesting=True)
        row = ['3', 
               '$2b$15$Kk3zHbZyk9q2K4skrd/47OvPtG/KBoE41TftO6xwO0Tz7cIgJlj46', 
               '171.66.16.37', 
               'get_course_info', 
               'view', 
               '{selected_course:105670, name:CS140}', 
               '{pinned:{}}', 
               '{course_info:#<Combo >, co_occurrences:{}, personal_distribution:[#<Combo EMPLID: 9668754>]}', 
               'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36', 
               '2015-10-24 07:59:37', 
               '2015-10-24 07:59:37']

        res = actlog_cleaner.extract_course_select(row, 1)
        expected = 105670
        self.assertEqual(res, expected)

    #------------------------------------
    # test_find_search
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_find_search(self):
        
        actlog_cleaner = ActivityLogCleaner(None, unittesting=True)
        # Start of search:
        row1 = ['5',
                'emplid1',
                '171.66.16.37',
                'find_search',
                'search',
                '{search_term:cs 1}' 
                ]
        row2 = ['6',
                'emplid1',
                '171.66.16.37',
                'find_search',
                'search',
                '{search_term:cs 101}' 
                ]
        row3 = ['7',
                'emplid2',
                '171.66.16.37',
                'find_search',
                'search',
                '{search_term:stats 4}' 
                ]

        actlog_cleaner.dispatch_row(row1, 5)
        actlog_cleaner.dispatch_row(row2, 6)
        actlog_cleaner.dispatch_row(row3, 7)
        
        search_entry = actlog_cleaner.crs_search_buf
        expected = [(5, 'cs 101')]

        self.assertListEqual(search_entry.arr, expected)
        
        row_id = actlog_cleaner.crs_search_state['row_id']
        emplid = actlog_cleaner.crs_search_state['emplid']
        self.assertTupleEqual((row_id, emplid), (7, 'emplid2'))
        self.assertEqual(actlog_cleaner.search_term_accumulator, 'stats 4')
        
    #------------------------------------
    # test_extract_pins 
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_extract_pins(self):
        
        early_pin = ['0','1','2','3','4','5',
                     ('Some stuff\t{pinned:{1156:208582, 1162:120904}, course_history_ids:[102794, '
                     '105644, 105645, 105649, 105687, 105730, 111845 and More')]
        
        late_pin = ['0','1','2','3','4','5',
                    ('foobar "pinned_courses"=>[#<Enrollment STRM: 1214, CLASS_NBR: 25600, CRSE_ID: 204608, '
                     'CATALOG_NBR: "151", SUBJECT: "ARCHLGY", DESCRIPTION: "Ten Things: An Archaeology '
                     'of Design (CLASSICS 151...">, #<Enrollment STRM: 1215, CLASS_NBR: 18259, '
                     'CRSE_ID: 105670, CATALOG_NBR: "140", SUBJECT: "CS", '
                     'DESCRIPTION: "Operating Systems and Systems Programming">, '
                     '#<Enrollment STRM: 1214, CLASS_NBR: 18476, CRSE_ID: 105687, '
                     'CATALOG_NBR: "161", SUBJECT: "CS", DESCRIPTION: '
                     '"Design and Analysis of Algorithms">] maybe more stuff')]
        
        empty_pin = ['0','1','2','3','4','5','Nothing There']

        actlog_cleaner = ActivityLogCleaner(None, unittesting=True)
        
        actlog_cleaner.extract_pins(early_pin, 10)
        expected = [(10, 1156, 208582), (10, 1162, 120904)]
        res = actlog_cleaner.pins_in_context_buf.arr
        
        self.assertListEqual(res, expected)
        
        # Late-version pins:
        actlog_cleaner.pins_in_context_buf.truncate()
        
        actlog_cleaner.extract_pins(late_pin, 20)
        expected = [(20, 1214, 204608), (20, 1215, 105670), (20, 1214, 105687)]
        res = actlog_cleaner.pins_in_context_buf.arr
        
        self.assertListEqual(res, expected)

        actlog_cleaner.pins_in_context_buf.truncate()
        
        actlog_cleaner.extract_pins(empty_pin, 30)
        expected = []
        res = actlog_cleaner.pins_in_context_buf.arr
        
    #------------------------------------
    # test_extract_enrl_history
    #-------------------
        
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_extract_enrl_history(self):
        
        early_enrl = ['0','1','2','3','4','5',
                      'Some stuff course_history_ids:[102794, 105644, 105645, 105649], more stuff']
        late_enrl  = ['0','1','2','3','4','5',
                      ('Some stuff {"registered_courses"=>[#<Enrollment STRM: nil, CLASS_NBR: nil, '
                       'CRSE_GRADE_OFF: "A-", CRSE_ID: 105687, CATALOG_NBR: "161", '
                       'SUBJECT: "CS", DESCR: "DESIGN & ANALYSIS ALGORITHMS", DESCRIPTION: '
                       '"Design and Analysis of Algorithms">, #<Enrollment STRM: nil, CLASS_NBR: '
                       'nil, CRSE_GRADE_OFF: "B+", CRSE_ID: 219885, CATALOG_NBR: "230", SUBJECT: "CS", '
                       'DESCR: "DEEP LEARNING", DESCRIPTION: "Deep Learning">More stuff]')]
        
        actlog_cleaner = ActivityLogCleaner(None, unittesting=True)
        actlog_cleaner.extract_enrl_history(early_enrl, 10)
        
        expected = [(10, 102794), (10, 105644), (10, 105645), (10, 105649)]
        res = actlog_cleaner.enrl_hist_buf.arr
        self.assertListEqual(res, expected)
        
        res = actlog_cleaner.enrl_hist_buf.truncate()
        actlog_cleaner.extract_enrl_history(late_enrl, 20)
        
        expected = [(20, 105687), (20, 219885)]
        res = actlog_cleaner.enrl_hist_buf.arr 
        self.assertListEqual(res, expected)

    #------------------------------------
    # test_ip_lookup
    #-------------------
    
    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_ip_lookup(self):
        
        ip_dict = IpFullLocation(ipTablePath=None)
        ip_loc = ip_dict.get('123.456.789.012')
        expected = ('CN', 'China', 'Beijing', 'Beijing', 39.9075, 116.39723, '100006', '+08:00', '86', '010')
        self.assertTupleEqual(ip_loc, expected)
        # Intentionally not found:
        ip_loc = ip_dict.get('000.000.789.012')
        self.assertIsNone(ip_loc)
        # Not found with default:
        ip_loc = ip_dict.get('000.000.789.012',
                             default=ActivityLogCleaner.DEFAULT_IPLOC_TUPLE)
        self.assertTupleEqual(ip_loc, ActivityLogCleaner.DEFAULT_IPLOC_TUPLE)

    #------------------------------------
    # test_insert_ip_loc
    #-------------------
    
    #*******@unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_insert_ip_loc(self):
        
        actlog_cleaner = ActivityLogCleaner(None, unittesting=True)
        
        # IP of whale.stanford.edu:
        ip_loc_tuple = actlog_cleaner.ip_dict.lookupIP('171.64.75.72')
        actlog_cleaner.buffer(actlog_cleaner.ip_location_buf, ip_loc_tuple)
        
        expected = [('US', 
                     'United States', 
                     'California', 
                     'Stanford', 
                     37.421262, 
                     -122.163949, 
                     '94305', 
                     '-07:00', 
                     '1', 
                     '650')]

        self.assertListEqual(actlog_cleaner.ip_location_buf.arr, expected)

        actlog_cleaner.ip_location_buf.truncate()
        
        ip_loc_tuple = actlog_cleaner.ip_dict.get('000.000.75.72',
                                                  actlog_cleaner.DEFAULT_IPLOC_TUPLE
                                                  )
        actlog_cleaner.buffer(actlog_cleaner.ip_location_buf, ip_loc_tuple)
        self.assertListEqual(actlog_cleaner.ip_location_buf.arr,
                             [actlog_cleaner.DEFAULT_IPLOC_TUPLE]
                             )


# --------------------- Main ----------------
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()