COMPLETED_FILE_REVIEWS = """
SELECT COUNT(*) 
FROM file_reviews
 WHERE project_name = ':name'
  AND review_status = 'completed'
"""

UNCOMPLETED_FILE_REVIEWS = """
SELECT COUNT(*) 
FROM file_reviews
 WHERE project_name = ':name'
  AND review_status = 'uncompleted'
"""

LOANS_WITH_EXCEPTIONS = """
SELECT COUNT(*)
FROM reviewed_loans
 WHERE project_name = ':name'
  AND exception_status = 'exception'
"""

LOANS_WITHOUT_EXCEPTIONS = """
SELECT COUNT(*)
FROM reviewed_loans
 WHERE project_name = ':name'
  AND exception_status = 'no exception'
"""

LOAN_DOCUMENT_TYPE_AND_STATUS = """
SELECT COUNT(*)
FROM reviewed_loans
 WHERE project_name = ':name'
  AND document_type = ':type'
  AND document_status = ':status'
"""

AVG_FILE_REVIEW_TIME = """
SELECT start_time, end_time
 TO_CHAR(
     start_time,
     'MON-DD-YYYY HH12:MIPM'
 )
 TO_CHAR(
     end_time,
     'MON-DD-YYYY HH12:MIPM'
 )
FROM file_reviews
 WHERE project_name = ':name'
  AND document = ':document'
  AND agent = ':agent'
"""