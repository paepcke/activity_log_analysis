'''
Created on Nov 24, 2021

@author: paepcke
'''

import csv
import getpass
import os

import explorecourses
from logging_service import LoggingService
from pymysql_utils.pymysql_utils import MySQLDB

# For running in Eclipse on Mac: add path to mysql client:
#**********
os.environ['PATH'] = '/usr/local/bin/:' + os.environ['PATH']
#**********


class SupportTablePuller:
    '''
    classdocs
    '''
    
    ACTLOG_DIR = '/Users/paepcke/Project/Carta/Data/CartaData/ActivityLog'
    DB_NAME = 'activity_log'

    #------------------------------------
    # Constructor
    #-------------------


    def __init__(self,
                 db_user=None,
                 db_name=None,
                 db_pwd=None,        # ******Remove 
                 ):
        '''
        Constructor
        '''
        connection = None
        
        self.log = LoggingService()
        
        # Connections between course subjects, departments, schools, and subschools:
        
        schools_and_departments_csv_path = os.path.join(self.ACTLOG_DIR, 
                                                        'SupportTables/schools_and_departments.csv') 
        if not os.path.exists(schools_and_departments_csv_path):
            self.log.info(f"File SupportTables/schools_and_departments.csv does not exist, creating it...")
            if connection is None:
                connection = explorecourses.CourseConnection()
            
            self.log.info(f"Pulling schools from ExploreCourses...")
            self.schools = connection.get_schools('')
            self.log.info(f"Done pulling; saving to file...")
            self._schools_to_csv(self.schools, schools_and_departments_csv_path)
            self.log.info(f"Done writing SupportTables/schools_and_departments.csv")
        else:
            self.log.info(f"File SupportTables/schools_and_departments.csv exists; using it.")

        subj_school_subschool_path = os.path.join(self.ACTLOG_DIR, 
                                                  'subj_school_subschool_department.tsv') 
        if os.path.exists(subj_school_subschool_path):
            self.log.info(f"Removing file {subj_school_subschool_path}")
            os.remove(subj_school_subschool_path)

        # Create top level table connecting subject with 
        # School, department and subschool:
        self.course_info_dict = self.create_subj_school_subschool_csv(subj_school_subschool_path)
        
        # Info about courses, pulled from ExploreCourses:
        
        course_tbl_path = os.path.join(self.ACTLOG_DIR, 'course_info.tsv')

        if not os.path.exists(course_tbl_path):
            self.log.info(f"File course_info.csv does not exist; pulling from ExploreCourses...")
            if connection is None:
                connection = explorecourses.CourseConnection()
            self.pull_course_info(connection, course_tbl_path)
        else:
            self.log.info(f"File course_info.csv exists; using it")

        self.open_db(db_user, db_name, db_pwd)

        # Load subj_school_subschool_department.csv into db:
        self.load_table(subj_school_subschool_path, 'SubjSchoolSubschoolDep')
        # Same with course_info:
        self.load_table(course_tbl_path, 'CourseInfo')

    #------------------------------------
    # create_subj_school_subschool_csv
    #-------------------
    
    def create_subj_school_subschool_csv(self, outfile):
        '''
        Creates CSV file for lookup up School, Subschool, and department
        names from course 'subject':
          'subject', 'department_name', 'school_name', 'subschool', 'subschool_short'

        The file is constructed by merging two files:
           schools_and_departments.csv, and
           school_subparts.csv
           
        The first contains:
              school_name                , department_name, department_code'
              Graduate School of Business,    Accounting,        ACCT
              
       The school_subparts.csv is like:
        
	        "subject","department","subschool","acad_group","subschool_short"
	        "ACCT","GSB","Graduate School of Business","GSB","GSB"
	        "ARTSINST","SAI","H&S Interdisciplinary Programs","HUMSCI","H&SInterdisc"
	        "CS","COMPUTSCI",\\N,"ENGINEER","ENGINEER"
            "CS","ELECTENGR",\\N,"ENGINEER","ENGINEER"
	            ...
        
            subject  school_name  department_name  subschool   subschool_short
        
        '''

        # Build a dict department name to School name:
        dep_to_school = {}
        schools_departments_file = os.path.join(self.ACTLOG_DIR, 
                                                'SupportTables/schools_and_departments.csv')
        with open(schools_departments_file, 'r') as fd:
            reader = csv.DictReader(fd)
            for school_dep in reader:
                dep_code = school_dep['department_code']
                # Department to school name and department name
                dep_to_school[dep_code] = (school_dep['school_name'], 
                                           school_dep['department_name']) 

        course_info = []
        with open(os.path.join(self.ACTLOG_DIR, 'SupportTables/school_subparts.csv'), 'r') as fd:
            reader = csv.DictReader(fd)
            for row_dict in reader:
                subj =  row_dict['subject']
                # Does the subject match a department name?
                try:
                    school_name, dep_name = dep_to_school[subj]
                except KeyError:
                    acad_group = row_dict['acad_group']
                    try:
                        school_name, dep_name = dep_to_school[acad_group]
                    except KeyError:
                        # Really not found. Happens for courses whose
                        # schools or departments no longer exist
                        continue
                subschool = row_dict['subschool']
                if subschool == '\\N':
                    subschool = 'null'
                subschool_short = row_dict['subschool_short']
                if subschool_short == '\\N':
                    subschool_short = 'null'

                course_info.append([subj, dep_name, school_name, subschool, subschool_short])

        self.log.info(f"Writing subj-depNm-schoolNm-subschNm-subschShrt to {outfile}...")
        with open(outfile, 'w') as fd:
            writer = csv.writer(fd, delimiter='\t')
            writer.writerow(['subject', 'department_name', 'school_name', 'subschool', 'subschool_short'])
            for row in course_info:
                writer.writerow(row)
        self.log.info(f"Done riting subj-depNm-schoolNm-subschNm-subschShrt.")

        # Create a map subject->('department_name', 'school_name', 'subschool', 'subschool_short')
        course_info_dict = {subject : (department_name, school_name, subschool, subschool_short)
                            for subject, department_name, school_name, subschool, subschool_short
                            in course_info}
        return course_info_dict

    #------------------------------------
    # pull_course_info
    #-------------------
    
    def pull_course_info(self, connection, course_tbl_path):
        '''
        
        Each course from ExploreCourses contains the following information:
       		{'year': '2021-2022',
    		 'subject': 'AA',
    		 'code': '47SI',
    		 'title': 'Why Go To Space?',
    		 'description': 'Why do we spend billions of dollars exploring space? What can modern policymakers,
    		                 and geopolitical considerations.',
    		 'gers': ('',),
    		 'repeatable': False,
    		 'grading_basis': 'Satisfactory/No Credit',
    		 'units_min': 1,
    		 'units_max': 1,
    		 'objectives': (),
    		 'final_exam': False,
    		 'sections': (),
    		 'tags': (<explorecourses.classes.Tag object at 0x7fe9502c6f40>,),
    		 'attributes': (<explorecourses.classes.Attribute object at 0x7fe9502c6f70>,),
    		 'course_id': 217539,
    		 'active': True,
    		 'offer_num': '1',
    		 'academic_group': 'ENGR',
    		 'academic_org': 'AEROASTRO',
    		 'academic_career': 'UG',
    		 'max_units_repeat': 1,
    		 'max_times_repeat': 1
             }

        :param connection:
        :type connection:
        :param course_tbl_path:
        :type course_tbl_path:
        '''

        with open(course_tbl_path, 'w') as fd:
            writer = csv.writer(fd, delimiter='\t')
            writer.writerow(['crs_id', 
                             'acad_yr', 
                             'subject', 
                             'catalog_nbr', 
                             'crs_code', 
                             'crs_title', 
                             'crs_description', 
                             'gers', 
                             'grading_basis', 
                             'acad_group', 
                             'acad_org'])
            self.log.info(f"Pulling/writing crs info from ExploreCourses to {course_tbl_path}...")
            for department in self.course_info_dict.keys():
                for year in range(2015, 2022):
                    # Year specs are of the form 2016-2017
                    yr_spec = f"{year}-{year+1}"
                    courses = connection.get_courses_by_department(department, year=yr_spec)
                    
                    for course in courses:
                        row = [
                            course.course_id,
                            course.year,
                            course.subject,
                            course.code,
                            f"{course.subject} {course.code}", # crs_code
                            course.title,
                            course.description,
                            course.gers,
                            course.grading_basis,
                            course.academic_group,
                            course.academic_org
                            ]
                        writer.writerow(row)
        self.log.info(f"Done pulling/writing crs info from ExploreCourses to {course_tbl_path}.")                        


    #------------------------------------
    # load_table
    #-------------------
    
    def load_table(self, infile, dest_tbl):

        self.log.info(f"Loading tbl {dest_tbl} from {infile}")
        (errs, warns) = self.db.execute(f'''LOAD DATA LOCAL INFILE '{infile}'
                  INTO TABLE {dest_tbl}
                  FIELDS TERMINATED BY '\t' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n'
                  IGNORE 1 LINES;
                  '''
        )
        if errs is not None:
            self.log.err(f"Errors reading {infile} into tbl {dest_tbl}: {errs}")
        if warns is not None:
            self.log.err(f"Warnings reading {infile} into tbl {dest_tbl}: {warns}")

        self.log.info(f"Done loading tbl {dest_tbl} from {infile}")

    #------------------------------------
    # open_db
    #-------------------

    def open_db(self, uname=None, db_name=None, pwd=None, start_fresh=False):

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

        self.log.info(f"Ensuring table presences in db...")
        # Test whether all necessary tables exist:
        for tbl_nm in ['CourseInfo', 'SubjSchoolSubschoolDep']:
            res = db.query(f'''SELECT COUNT(*)
                                FROM information_schema.tables 
                               WHERE table_schema = "{self.DB_NAME}"
                                 AND table_name = "{tbl_nm}";'''
            )
            if next(res) == 0:
                self.create_tbl(tbl_nm)
            
            # Truncate table if starting over:
            if start_fresh:
                db.truncateTable(tbl_nm)
        self.log.info(f"Done ensuring table presences in db...")
        return db


    #------------------------------------
    # create_tbl
    #-------------------
    
    def create_tbl(self, tbl_nm):
        
        self.log.info(f"Creating tbl {tbl_nm} in db...")

        if tbl_nm == 'CourseInfo':
            schema = {
                    'crs_id' : 'int',
                    'acad_yr' : 'varchar(9)',
                    'subject' : 'varchar(20)',
                    'catalog_nbr' : 'varchar(15)',
                    'crs_code' : 'varchar(50)',
                    'crs_title' : 'varchar(200)',
                    'crs_description' : 'varchar(9000)',
                    'gers' : 'varchar(100)',
                    'grading_basis' : 'varchar(40)',
                    'acad_group' : 'varchar(40)',
                    'acad_org' : 'varchar(40)'
                    }
           
        elif tbl_nm == 'SubjSchoolSubschoolDep':
            schema = {
                    'subject'         : 'varchar(20)',
                    'department_name' : 'varchar(50)',
                    'school_name'     : 'varchar(50)',
                    'subschool'       : 'varchar(50)',
                    'subschool_short' : 'varchar(50)'
                    }
        else:
            raise NotImplementedError(f"Don't know how to create tbl '{tbl_nm}'")
        
        self.db.createTable(tbl_nm, schema)
        self.db.execute(f"ALTER TABLE {tbl_nm} engine=MyISAM")

        self.log.info(f"Done creating tbl {tbl_nm} in db.")

    #------------------------------------
    # _schools_to_csv
    #-------------------
    
    def _schools_to_csv(self, school_list, outfile):
        
        with open(outfile, 'w') as fd:
            writer = csv.writer(fd, delimiter=',')
            writer.writerow(['school_name', 'department_name', 'department_code'])
            
            for school in school_list:
                school_name = school.name
                for department in school.departments:
                    writer.writerow([school_name, department.name, department.code])



# ------------------------ Main ------------
if __name__ == '__main__':
    
    SupportTablePuller()
    