from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker, declarative_base

class DataDumper:
    def __init__(self, source_conn_string, dest_conn_string, source_table_name, dest_table_name):
        self.source_engine = create_engine(source_conn_string)
        self.dest_engine = create_engine(dest_conn_string)
        
        self.SessionSource = sessionmaker(bind=self.source_engine)
        self.SessionDestination = sessionmaker(bind=self.dest_engine)
        
        self.source_table_name = source_table_name
        self.dest_table_name = dest_table_name
        
        self.Base = declarative_base()

    def setup_tables(self):
        self.metadata_source = MetaData()
        self.metadata_destination = MetaData()

        self.tbl_source = Table(self.source_table_name, self.metadata_source, autoload_with=self.source_engine)
        self.tbl_destination = Table(self.dest_table_name, self.metadata_destination, autoload_with=self.dest_engine)

        class SourceTable(self.Base):
            __table__ = self.tbl_source

        class DestinationTable(self.Base):
            __table__ = self.tbl_destination

        self.SourceTable = SourceTable
        self.DestinationTable = DestinationTable

    def dump_data(self, batch_size=1000):
        session_source = self.SessionSource()
        session_destination = self.SessionDestination()

        try:
            self.setup_tables()
            inspector = inspect(self.source_engine)
            columns = [col["name"] for col in inspector.get_columns(self.source_table_name)]

            total_rows = session_source.query(self.SourceTable).count()
            print(f"Total rows to process: {total_rows}")

            for i in range(0, total_rows, batch_size):
                batch = session_source.query(self.SourceTable).limit(batch_size).offset(i).all()

                values = [
                    {col: getattr(item, col) for col in columns}
                    for item in batch
                ]

                session_destination.execute(self.DestinationTable.__table__.insert().values(values))
                session_destination.commit()
                print(f"Batch from {i} to {i + batch_size} processed.")
        except Exception as e:
            session_destination.rollback()
            print(f"An error occurred: {e}")
        finally:
            session_source.close()
            session_destination.close()

# Exmaple of usage
source = 'connection string for the source '
destination = 'connection string for the destination'
source_table = 'source table'
destination_table = 'destination table'

migrator = DataDumper(source, destination, source_table, destination_table)
migrator.dump_data(batch_size=1000)
