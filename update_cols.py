from settings import VM

cursor = VM.cursor()

query = """UPDATE 
        public.ibea_agregate
    SET 
        line = 'LZ-02 ST', line_side = 'LZ-02 ST'
    WHERE 
        line = 'LZ-2 ST' 
        AND line_side = 'LZ-2 ST'"""

cursor.execute(query)
VM.commit()