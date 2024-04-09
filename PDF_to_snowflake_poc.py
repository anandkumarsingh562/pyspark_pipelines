from PyPDF2 import PdfReader
import pandas as pd
import snowflake.connector;
from snowflake.sqlalchemy import URL;
from sqlalchemy import create_engine;


names=[]
email=[]
phone=[]

for j in [1,7,8]:
    raw_data = PdfReader(rf"C:\Users\xavier_dp\Downloads\ANAND_{j}_AUG.pdf")
    names.append(raw_data.pages[0].extract_text().split("\n")[0])

    for i in raw_data.pages:
        data = i.extract_text()
        data_words=data.split("\n")
        for k in data_words:
            if k.startswith("E-Mail"):
                val=k.split(":")[1]
                email.append(val)
            if k.startswith("Phoneno"):
                va=k.split("-")
                print(k.split("–"))
                phone.append(va[0])

Resume_data=[names,email,phone]

pdf_df=pd.DataFrame(Resume_data).transpose()

pdf_df.columns=["name","email","phone"]

engine = create_engine(URL(account = "XXXXXXXX",user = "XXXXXXXX",password = "XXXXXX",database="my_db",schema="anand_sch",role="XXXXXX",warehouse="D1"))

conn=engine.connect()

pdf_df.to_sql("RESUME_DATA_TABLE",if_exists="append",con=conn,index=False)

conn.close()
engine.dispose()

#print(names)
#print(email)
#print(phone)

#Phoneno6290976960


    
        
        
    
    

