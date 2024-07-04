from sqlalchemy import create_engine, MetaData, Table, inspect, func
from sqlalchemy.orm import sessionmaker, declarative_base

class DataMigrator:
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

    def migrate_data(self, chunk_size=10000):
        session_source = self.SessionSource()
        session_destination = self.SessionDestination()

        try:
            self.setup_tables()
            inspector = inspect(self.source_engine)
            columns = [col["name"] for col in inspector.get_columns(self.source_table_name)]
            
            max_id = session_destination.query(func.max(self.DestinationTable.id)).scalar() or 0
            total_records = session_source.query(func.count(self.SourceTable.id))\
                .filter(self.SourceTable.id > max_id).scalar()

            print(f"Total records to insert: {total_records}")

            # Calculate the number of chunks
            num_chunks = (total_records + chunk_size - 1) // chunk_size  # Ceiling division

            for i in range(num_chunks):
                # Fetch the next chunk of records
                batch = session_source.query(self.SourceTable)\
                    .filter(self.SourceTable.id > max_id)\
                    .order_by(self.SourceTable.id)\
                    .limit(chunk_size)\
                    .all()

                if batch:
                    values = [
                        {col: getattr(item, col) for col in columns}
                        for item in batch
                    ]

                    # Insert the chunk
                    session_destination.execute(self.DestinationTable.__table__.insert().values(values))
                    session_destination.commit()

                    # Update max_id for the next batch
                    max_id = batch[-1].id

                    print(f'Chunk {i+1}/{num_chunks}: Inserted {len(batch)} records up to ID {max_id}')
                else:
                    break

        except Exception as e:
            session_destination.rollback()
            print(f"An error occurred: {e}")
        finally:
            session_source.close()
            session_destination.close()

source = "source connection string"
destination = "destination connection string"
source_table = 'source table'
destination_table = 'destination_table'

migrator = DataMigrator(source, destination, source_table, destination_table)
migrator.migrate_data(chunk_size=10000)
