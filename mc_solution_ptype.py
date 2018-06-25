from ConfigParser import ConfigParser

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from sql_queries import (
    COMPLETED_FILE_REVIEWS, UNCOMPLETED_FILE_REVIEWS, LOANS_WITH_EXCEPTIONS,
    LOANS_WITHOUT_EXCEPTIONS, LOAN_DOCUMENT_TYPE_AND_STATUS, AVG_FILE_REVIEW_TIME
)


class DocumentType(object):
    def __init__(self, document_type):
        self.type = document_type


class DocumentStatus(object):
    def __init__(self, document_status):
        self.status = document_status


class DocumentException(object):
    EXCEPTION_MAPPING = {
        ('Mortgage', 'Incomplete'): 'Exception_01',
        ('Certification of Title', 'Copy Not Recorded'): 'Exception_02',
        ('Power of Attorney', 'Filed with the Court'): 'Exception_03',
    }

    def __init__(self, document_type, document_status):
        self.document_type = DocumentType(document_type)
        self.document_status = DocumentStatus(document_status)

    def trigger_exception(self):
        except_attrs = (self.document_type.type, self.document_status.status)
        for attrs, exception in DocumentException.EXCEPTION_MAPPING.iteritems():
            if except_attrs != attrs:
                continue
            return exception
        else:
            return 'No exception found'


class DBConnection(object):
    def __init__(self):
        config = ConfigParser()
        config.read('db_config.ini')
        self.__db = dict(config.items('postgres'))
        self.__engine = create_engine(
            'postgresql+psycopg2://{username}:{password}@{hostname}/{database}'.format(
                    username=self.__db['user'],
                    password=self.__db['password'],
                    hostname=self.__db['host'],
                    database=self.__db['dbname']))

    @property
    def engine(self):
        return self.__engine

    def __repr__(self):
        return 'DBConnection() => postgresql+psycopg2://{0}:{1}@{2}/{3}'.format(
            self.__db['user'],
            self.__db['password'],
            self.__db['host'],
            self.__db['dbname'])


class GenericProject(object):
    def __init__(self, project_name, doc_type, doc_status):
        self.project_name = project_name
        self.doc_type_obj = DocumentType(doc_type)
        self.doc_status_obj = DocumentStatus(doc_status)
        self.db = DBConnection().engine

    def get_completed_reviews(self):
        return self.__filterby_project_name_query(COMPLETED_FILE_REVIEWS)

    def get_uncompleted_reviews(self):
        return self.__filterby_project_name_query(UNCOMPLETED_FILE_REVIEWS)

    def get_loans_with_exceptions(self):
        return self.__filterby_project_name_query(LOANS_WITH_EXCEPTIONS)

    def get_loans_without_exceptions(self):
        return self.__filterby_project_name_query(LOANS_WITHOUT_EXCEPTIONS)

    def get_loans_by_type_n_status(self):
        with self.db.connect() as cnxn:
            query = text(LOAN_DOCUMENT_TYPE_AND_STATUS)
            query = query.bindparams(
                name=self.project_name,
                type=self.doc_type_obj.type,
                status=self.doc_status_obj.status)
            list_of_rows = cnxn.execute(query)
        return list_of_rows

    def get_avg_file_review_time(self, document, agent):
        with self.db.connect() as cnxn:
            query = text(AVG_FILE_REVIEW_TIME)
            query = query.bindparams(
                name=self.project_name,
                document=document,
                agent=agent)
            list_of_rows = cnxn.execute(query)

        start_times = [row[0] for row in list_of_rows]
        end_times = [row[1] for row in list_of_rows]

        df = pd.DataFrame({'start_time': start_times, 'end_time': end_times})

        df['start_time'] = df.start.map(self.__convert_to_timestamp)
        df['end_time'] = df.end.map(self.__convert_to_timestamp)

        df['average_time'] = df.start + (df.end - df.start) / len(list_of_rows)
        return df.average_time

    def __filterby_project_name_query(self, query_string):
        with self.db.connect() as cnxn:
            query = text(query_string)
            query = query.bindparams(name=self.project_name)
            list_of_rows = cnxn.execute(query).fetchall()
        return list_of_rows

    @staticmethod
    def __convert_to_timestamp(time_string):
        try:
            return pd.Timestamp(time_string)
        except:
            return np.nan
