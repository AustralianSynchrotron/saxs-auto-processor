import sys
import logging
try:
    import yaml
    from sqlalchemy import create_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import Column, Integer, String
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    # sys.exit(2) #this line terminated sphinx docs building on readthedocs.

class TableBuilder():

    def __init__(self, config, database_name, tableName, attribList):
        self.name = "TableBuilder"
        self.config = config
        
        self.Base = declarative_base()
        self.tableName = tableName
        #self.engine = create_engine('sqlite:///'+str(location)+str(user)+'.db')
        self.engine = self.createDBEngine(database_name)
        self.dictColumns = {}
        self.attribList = self.columnBuilder(attribList)
        self.table = self.gen(self.tableName, self.dictColumns)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        
    def createDBEngine(self, database_name):
        database = self.config.get('database')
        
        engine_str = "mysql+mysqldb://%s:%s@%s/%s" % (database['user'], database['passwd'], database['host'], database_name)
        db_engine = create_engine(engine_str)
        
        return db_engine
    
    def gen(self, table_name, colDict):
        class logDataTableBuilder(self.Base):
            locals().update(colDict)  
            __tablename__ = table_name
            id = Column(Integer, primary_key=True)
    
        self.Base.metadata.create_all(self.engine)
        return logDataTableBuilder

    def columnBuilder(self, attribList):
        for attribute in attribList:
            self.dictColumns[attribute] = Column(String(250))
                 
    def addData(self, data):
        newData = self.table(**data)
        #self.session.add(newData)
        #self.session.commit()
        
        instance = None
        if self.tableName == 'subtracted_images':
            instance = self.session.query(self.table).filter_by(subtracted_location=newData.subtracted_location).first() 
        elif self.tableName == 'buffers':
            instance = self.session.query(self.table).filter_by(buffer_location=newData.buffer_location).first() 
        elif self.tableName == 'average_images':
            instance = self.session.query(self.table).filter_by(average_location=newData.average_location).first() 
        elif self.tableName == 'average_subtracted_images':
            instance = self.session.query(self.table).filter_by(average_subtracted_location=newData.average_subtracted_location).first() 
        
        if not instance:
            self.session.add(newData)
            self.session.commit()
        

if __name__ == "__main__":
    
    print "winner"
    collumAttributes = ['WashType', 'SampleOmega', 'FilePluginDestination', 'Temperature2', 'Temperature1', 'WellNumber', 'SamplePhi', 'NumericTimeStamp', 'I0', 'SampleY', 'SampleX', 'SampleChi', 'TimeStamp', 'SampleType', 'ImageCounter', 'Ibs', 'exptime', 'FilePluginFileName', 'Energy', 'It', 'SampleTableX', 'SampleTableY', 'NORD', 'ImageLocation']
    t = TableBuilder("JAck", "images", collumAttributes)

    
     
    
