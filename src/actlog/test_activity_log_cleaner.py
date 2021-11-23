'''
Created on Nov 22, 2021

@author: paepcke
'''
import os
import unittest

from src.actlog.activity_log_cleaning import ActivityLogCleaner


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
    
    #*******@unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_init_db(self):
        
        actlog_cleaner = ActivityLogCleaner(None, unittesting=True)
        actlog_cleaner.open_db()
        print('foo')


    #------------------------------------
    # test_buffer_int_array
    #-------------------

    @unittest.skipIf(TEST_ALL != True, 'skipping temporarily')
    def test_buffer_int_array(self):
        
        str_arr = b'[123, 456]'
        res_arr = []
        expected = [(1,123), (1, 456)]
        
        actlog_cleaner = ActivityLogCleaner(None, unittesting=True)
        actlog_cleaner.buffer_int_arr(res_arr, 1, str_arr)
        
        self.assertListEqual(res_arr, expected)
        
        # No space after comma:
        str_arr = b'[123,456]'
        res_arr = []
        actlog_cleaner.buffer_int_arr(res_arr, 1, str_arr)
        
        self.assertListEqual(res_arr, expected)

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

        actlog_cleaner.extract_course_select(row, 1)
        
        res = actlog_cleaner.crse_selects_buf
        expected = [(1, 105670)]
        self.assertListEqual(res, expected)


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
        
        search_entry = actlog_cleaner.crse_search_buf
        expected = [(5, 'cs 101')]

        self.assertListEqual(search_entry, expected)
        
        self.assertTupleEqual(actlog_cleaner.crse_search_state, (7, 'emplid2'))
        self.assertEqual(actlog_cleaner.search_term_accumulator, 'stats 4')
        

# --------------------- Main ----------------
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()