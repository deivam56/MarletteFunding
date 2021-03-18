import logging

from database import Database

logging.basicConfig(level="DEBUG")
logger = logging.getLogger(__name__)


class EtlScript:
    def __init__(self):
        self.database_conn = Database("acme")
        self.header_file = "headers.txt"
        self.data_file = "data.tsv"
        self.out_file = "output.csv"

    def load_file_to_database(self, file_path: str):
        self.database_conn.load_file(file_path)

    def run(self):
        # Your code starts here
        # constant labels
        comma_separator = ','
        pipe_separator = '|'
        read_file = 'r'
        write_file = 'w'
        new_line = '\n'
  
        # Store the header columns in a list
        header_columns = []
        try:
            header_fp = open(self.header_file, read_file )
        except Exception as ex:
            logger.error( f'Error: Can\'t open the header file: {ex}' )
            exit(1)
        logger.info( f'INFO: Header file {self.header_file} opened successfully.')

        try:    
            while True:
                h_column = header_fp.readline()
                if not h_column:
                    break                
                header_columns.append( h_column )
        except Exception as ex:
            logger.error( f'Error: Header file Error: {ex}' )
            exit(1)
        logger.info( f'INFO: Header file {self.header_file} processed successfully.')

        # write the column header to the output file
        try:
            output_fp = open(self.out_file, write_file )
        except Exception as ex:
            logger.error( f'Error: Can\'t open data output file: {ex}' )
            exit(1)        
        logger.info( f'INFO: Output data file {self.out_file} opened successfully.')

        try:
            output_fp.write( comma_separator.join( column.strip() for column in header_columns ))
            output_fp.write( new_line )
        except Exception as ex:
            logger.error( f'Error: Can\'t write into data output file: {ex}' )
        logger.info( f'INFO: Header columns are written into {self.out_file} successfully.')

        # process data file
        try:
            data_fp = open(self.data_file, read_file)
        except Exception as ex:
            logger.error( f'Error: Can\'t read the data file : {ex}' )      
            exit(1)
        logger.info( f'INFO: Data file {self.data_file} opened successfully.')

        # record count for metrics 
        record_count = 0

        try:
            while True:
                data_record = data_fp.readline()
                if not data_record:
                    break
                output_fp.write(data_record.replace( pipe_separator, comma_separator )  )
                record_count += 1
                logger.info( f'INFO: Processing {record_count} records. ')                
            logger.info( f'INFO: Data file processed {record_count} records successfully!')

            data_fp.close()
            logger.info( f'INFO: Input data file {self.data_file} closed.')

            output_fp.close()       
            logger.info( f'INFO: Output data file {self.out_file} successfully written.')
        except Exception as ex:
            logger.error( f'Error: Can\'t write into output file {self.out_file} : {ex}' )      
            exit(1)

        # Load data file into the Database
        try:        
            self.load_file_to_database(self.out_file)
        except Exception as ex:
            logger.error( f'Error: Can\'t write into the Database : {ex}' )              
        logger.info( f'INFO: Data file loaded into the Database.')

        logger.info( f'INFO: Data load ETL completed successfully.')
        exit(0)

if __name__ == "__main__":
    EtlScript().run()