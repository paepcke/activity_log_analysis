## Activity Log Exploration

Carta has maintained an activity log, which recorded high level visitor activities on the system. The log is semantically at a higher level than click streams in that entries are about activities, such as searching for a course or instructor, pinning, or unpinning courses. The log spans the time from Oct 2016 to Nov 2021, and contains 20,714,381 visitor activities

Each activity comes with related information. For example, a *pin* action log entry includes the course that was pinned, as well as the courses that the student has pinned earlier, and the courses in which they are enrolled at the time of the action.

The following access methods are available to analysts, in order of *time to skill level*, i.e. the amount and difficulty of required access technology: 

- Tableau natural language query (NLQ) access
- Tableau interactive visualization building
- Python/R access to the set of underlying database tables
- SQL access to the database tables

The natural language access is organized by analysis focus, such as origin and nature of an action, pinning and unpinning, enrollment, etc. Questions may include *"How many students?"* *"Pinned courses over time"*. *"Top 10 enrollments*". An [online tutorial](https://www.youtube.com/watch?v=27aIgkNyVa0) provides an in-depth introcudtion to this *Ask Data* facility. A [short getting-started section](#nql_getting_started] is provided below. The [NLQ access is available via the Web](https://us-west-2b.online.tableau.com/#/site/paepcke/datasources/15217696/askData).

The data from the activity log have been supplemented by information from Explore Courses, and location information by internet protocol address. These additional data are integrated in the activity datascape.

Figure 1 shows the activity log datascape (data model) ![Activity Log Datascape](readme_figs/datascapeCropped.png) as a set of interconnected tables.

The *Activities* table at the top contains one row for each action. The supplemental information for the actions is stored in the tables connected by blue links. The black links contain information from external sources. All blue-linked tables are connected via the *row_id* primary key in each table.

##Activities Table

| Column Name              |   Example Entry         |
|--------------------------|------------------------|
|    row_id   | 1 |
|    student  | $2b$15$Kk3zHbZyk9q2K4skrd/47OvPtG/KBoE41TftO6xwO0Tz7cIgJlj46 |
|    ip_addr  | 171.66.16.37 |
|   category  | piin |
|  action_nm  | pin |
| created_at  | 2015-10-24 07:56:58 |


The activity categories that are likely of interest are:

- find_search/detailed_search : searching in the Carta search box at the top.
- get_course_info : a visitor has clicked on a search result course to find
                  details about the course.
- pin/unpin : use of the Carta course pinning feature
- instructor_profile : searching for instructor information.

## Pinning-Related Tables

The *Pin* and *Unpin* tables contain the course being handled. The quarters during which the respective pin/unpin actions occurred are available only via the *created_at* column of the *Activities* table. Again, to obtain information about the action associated with a particular pin in this table, use the *row_id* key to find information such as the date of the pin, the visitor hash, and more. You are only concerned with this fact for access methods other than the natural language queries, which make that connection themselves. For the SQLers among you:


Given *Pins* table entry:

| Column Name   |  Example Entry |
|---------------|----------------|
| row_id        | 10354          |
| crs_id        | 105670         |

You could find out more via:

```
select * from Activities where row_id = 10354;
+--------+----------+---------------+-------------+-----------+---------------------+---------------------+
| row_id | student  | ip_addr       | category    | action_nm | created_at          | updated_at          |
+--------+----------+---------------+-------------+-----------+---------------------+---------------------+
|  10354 | $2b$1... | 10.31.192.169 | find_search | search    | 2015-10-26 01:18:11 | 2015-10-26 01:18:11 |
|  10354 | $2b$1... | 10.31.192.169 | find_search | search    | 2015-10-26 01:18:11 | 2015-10-26 01:18:11 |
+--------+----------+---------------+-------------+-----------+---------------------+---------------------|
```

Some actions contain a list of all courses pinned during the action. Those 'contextual' pins are available in the *ContextPins* table, which does include the quarter in which the pin occurred. It is structured just like the *Pins* table.

## Search-Related Tables

Only searches in the Carta course search box at the top of the interface are included in the *CrseSearchs* table. The table contains the search terms used. The action time in the associated *Activities* table *created_at* rows refer to the start of the visitor typing.

The *InstructorLookups* table contains the names of instructors for whom searches were entered in the search box at the top. Like this example:

| row_id | search_term |
|--------|-------------|
|   1158 | physics 41a |
|   1197 | spanlang 2a |
+--------+-------------+

## Enrollment

The best source for enrollment continues to be the *student_enrollment* table in the Carta main database. However, like for pins, a number of actions include the enrollment history of the acting visitor. the *EnrollmentHist* table contains those context history lists. These three entries from the *EnrollmentHist* table say "the visitor of action with *row_id* 10 in the *Activities* table was enrolled in three courses at the time they executed the logged action":

| row_id | crs_id |
|--------|--------|
|      10 | 102794 |
|      10 | 105644 |
|      10 | 105645 |

## Auxiliary tables

As seen in Figure 1, two tables external to the activity log information make queries more informative.

There an example from the *CourseInfo* table:

| Column Name          Example Entry
| ----------------- | ---------------------- |
| crs_id            | Six-digit course ID; use to tie to tables, such as *CrseSelection* |
| acad_yr           | 2015-2016 |
| subject           | AA |
| catalog_nbr       | 47SI |
| crs_code          | AA 47SI |
| crs_title         | Why Go To Space? |
| crs_description   | Why do we spend billions of dollars... |
| gers              | ('GER:DB-EngrAppSci', 'WAY-AQR', 'WAY-SMA') |
| grading_basis     | Satisfactory/No Credit |
| acad_group        | ENGR |
| acad_org          | AEROASTRO |


The *IpLocation* table includes information obout internet protocol address locations. This information is primarily of interest for summer and Covid-time visits to the Carta site. During normal times most visitors will be located at Stanford.

| Column Name        | Example Entry
|--------------------|-----------------|
|        row_id | 1 |
|  country_code | US |
|       country | United States |
|         state | California |
|          city | Stanford |
|           lat | 37.421262 |
|     longitude | -122.163949 |
|           zip | 94305 |
|     time_zone | -07 |00 |
| country_phone | 1 |
|     area_code | 650 |

Again, the natural language query facility is set up to make connections with the *crs_id* of the *CourseInfo* table, and the *row_id* of the *IpLocation* table automatic.

## NQL Getting Started

Tableau's natural language query facility is relatively new. All Carta activity tables have been [introduced into Tableau Ask Data](https://us-west-2b.online.tableau.com/#/site/paepcke/datasources/15217696/askData), and uploaded to a server where natural language queries are available.

NLQ does not process complicated language. The processor uses the table and column names, as well as some understanding of statistics and visualization style smarts to make sense of text that users type. That said, we introduced synonyms, so that alternative vocabulary will work as well. Rather than having to use 'crs_id in table CrseSelects,' which is an integer denoting a course, one can use the word 'course' instead.

Queries may build on each other. For example:

    "How many Students?"

produces the number of courses mentioned in the log. One might continue with:

    "by Year"

to get a time series line chart. If a barchart is prefered, one could continue with:

    "as a barchart"

The NQL interface is organized into *lenses*, which hide tables or fields unimportant to particular analysis tasks to help the analyst focus attention on just one inquiry.  The lenses are human-created, and can be changed. The above mentioned synonyms are associated with lenses. That is, each analysis task can have its own set of synonyms.

The change which lens to use, or to work in the NLQ using all tables at once, go back to the [initial
URL](https://us-west-2b.online.tableau.com/#/site/paepcke/datasources/15217696/askData).

If you are familiar with using Tableau Desktop, worksheets evolving from the queries can be downloaded and then developed futher. From within Tableau Desktop they can be downloaded to CSV for processing in R or Python. This means you'll need to get a free-for-EDU copy of Tableau Desktop.

### Tips and Cautions

Tableau's NLQ is still being developed, and can be finicky. 

- The best advice is to pay attention to the menus that pop down as you enter questions. Notice the table and fields to which each offer in the menu refers. You can consult the list of tables and fields on the left for the choice most likely to succeed.
- Note the pulldown menu on the upper right of the visualization pane. You can easily change to an alternative viz.
- Each question is shown in more stylized terms above the question box. Check those terms to see whehter the system grasped your intent. You can often pull down the boxes of the individual terms and reveal alterntives for you to choose, such as adding or removing from filters.
- Map Viz pulldown: Map, Text Table
- Country/City are based on the IP address of the Carta visitor, *not* from any university table.
- To pan maps: hold left mouse button for a couple of seconds, then move the mouse. Without the wait the move will result in a selection on the map.

### Example queries in the Course Search lens

    How many students by year?
       as barchart
    Activities by city
       <pull down viz menu on the upper right and select Text Table>
    Number of students by city
       filter City Aachen
       <remove the 'filter Aachen'>
       not City Aachen
    Students by Country
       exclude India
       <pull down the "filter Abc Country to India and select Germany>
       <pull down again and uncheck India>
    How many students
       by Country
       by Year
       <pull Viz menu near upper right; select Map>
    CrseSelects by Gers
       <Always pick the CrseSelects option for Gers> in the pull-down menu>
       Gers(CourseInfo1) contains "WAY-FR"
    Instructors
       by year
       <select Text Table from Viz selection pulldown>
       <left click on one of the Year column headers and select sort-by-down symbol>
    top 10 Instructors
       by Year

    CrseSearches
       filter Search Term "english"


