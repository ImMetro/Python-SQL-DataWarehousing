import pyodbc
import time
from datetime import date, datetime

def alter_smmcd3_table_schema(smmcd3_cursor, i, schema):
    smmcd3_schema = smmcd3_cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.Columns WHERE TABLE_NAME = {i}").fetchall()
    existing_columns_smmcd3 = [col.COLUMN_NAME for col in smmcd3_schema]
   
    # Compare ced3_schema/maze_schema with smmcd3_schema and alter smmcd3 if needed
    for col_schema in schema:
        if col_schema.COLUMN_NAME not in existing_columns_smmcd3:
            i = str(i.split("'")[1].split("'")[0])
            alter_query = f'ALTER TABLE {i} ADD {col_schema.COLUMN_NAME} varchar(255)'
            smmcd3_cursor.execute(alter_query)
            smmcd3_cursor.commit()
            print(alter_query)
            with open(f"./ExecutionLogs/{date.today()}-log.txt", '+a') as file:
                file.write(f"Altered smmcd3 schema: {alter_query}\n")
            file.close()
            
#     #    #    ####### #######    #     #                                                        ######                      
##   ##   # #        #  #          #     # #  ####  #####  ####  #####  #  ####    ##   #         #     #   ##   #####   ##   
# # # #  #   #      #   #          #     # # #        #   #    # #    # # #    #  #  #  #         #     #  #  #    #    #  #  
#  #  # #     #    #    #####      ####### #  ####    #   #    # #    # # #      #    # #         #     # #    #   #   #    # 
#     # #######   #     #          #     # #      #   #   #    # #####  # #      ###### #         #     # ######   #   ###### 
#     # #     #  #      #          #     # # #    #   #   #    # #   #  # #    # #    # #         #     # #    #   #   #    # 
#     # #     # ####### #######    #     # #  ####    #    ####  #    # #  ####  #    # ######    ######  #    #   #   #    # 
       
# ---- Tables to Import -----
tables_maze =  [
    "AF", "AKB", "AKC", "AKCT", "AKD", "AKK", "AKL", "AKLOG", "AR", "ARF", "ARLOG", "CR", "CRF", "CRFTC", 
    "DF", "DFB", "DFF", "DFNI", "DFVT", "DR", "DRF", "EF", "email",
    "EN", "ENMBD", "GL", "GLF", "GLFPREV", "GLPREV", "KAA", "KAB", "KAD", "KADM", "KAR", "KAS", "KASCED",
    "KBANK", "KBP", "KCA", "KCAB", "KCAR", "KCB", "KCC", "KCD", "KCE", "KCH", "KCL", "KCM", "KCP", "KCR",
    "KCS", "KCT", "KCY", "KDA", "KDCI", "KDIS", "KDR", "KES", "KFTC", "KGA", "KGC", "KGD", "KGE", "KGF",
    "KGH", "KGI", "KGJ", "KGL", "KGLT", "KGM", "KGN", "KGO", "KGP", "KGR", "KGS", "KGST", "KGT", "KGW",
    "KGX", "KGY", "KIS", "KKH", "KLF", "KMI", "KOA", "KOM", "KOP", "KPC", "KPCL", "KPCR", "KPE", "KPO",
    "KPQ", "KRA", "KREPORT", "KROLE", "KRR", "KSA", "KSC", "KSD", "KSF", "KSG", "KSGR", "KSI", "KSP", "KSQ",
    "KSS", "KTM", "OS", "OSAP", "OSNI", "OSQ", "OSVT", "OSYA", "PPD", "PPS", "QB", "QS", "SA", "SAB", "SABT",
    "SAD", "SADP", "SADW", "SAFT", "SAG", "SAI", "SAII", "SAIM", "SC", "SCC", "SCU", "SEK", "SEKD", "SEL",
    "SELK", "SF", "SFAQ", "SFAV", "SFEC", "SFGROUP", "SFMC", "SFPD", "SFQA", "SFRS", "SG", "SGAM", "SI", "SM",
    "SMAQ", "SMAV", "SMGROUP", "SPEMAIL", "SPEPRINT", "SPR", "SPRECIP", "SPREPLY", "SPRR", "SPSMS", "SR",
    "SRC", "SRCSU", "ST", "ST1", "ST2", "STAB", "STAW", "STBH", "STCA", "STEC", "STFR", "STLN", "STLS", "STMA",
    "STMBD", "STMC", "STPE", "STPM", "STQ", "STRA", "STSB", "STSP", "SU", "SUBL", "SUPR", "SXAS", "SXOS", "TC",
    "TCAT", "TCTB", "TCTD", "TCTQ", "TCTR", "TE", "TEC", "temp_CENETID_IDM", "temp_prevCEOID", "TETE", "TETN",
    "TH", "THTN", "THTQ", "TT", "TTEF", "TTEI", "TTEP", "TTES", "TTEX", "TTTG", "UM", "UMC", "usr_temp_ids"
]

start = time.time()
print(f"{date.today()}: Starting migration...")

# ----- Maze INSERT -----
for i in tables_maze:

    # ----- Connecting to Maze -----
    maze_server      = "redacted"
    maze_username    = "redacted"
    maze_password    = "redacted"


    maze_db     = 'DRIVER={SQL Server};SERVER='+maze_server+';UID='+maze_username+';PWD='+ maze_password+';DATABASE=redacted'
    maze        = pyodbc.connect(maze_db)
    print("Maze Connection: ", maze)

    smmcd3_server   = "redacted"
    smmcd3_username = "redacted"
    smmcd3_password = "redacted"

    smmcd3_db   = 'DRIVER={SQL Server};SERVER='+smmcd3_server+';UID='+smmcd3_username+';PWD='+ smmcd3_password+';DATABASE=redacted'
    smmcd3      =  pyodbc.connect(smmcd3_db)
    print("SMMD3 Connection: ", smmcd3)

    # ---- Cursor Setup ----

    maze_cursor = maze.cursor()
    smmcd3_cursor = smmcd3.cursor()

    table_name = "'"+i+"'"
    smmcd3_table_name = "'"+i+"_Maze'"

    # Grab Schema from source table
    schema = maze_cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.Columns WHERE TABLE_NAME = {table_name}").fetchall()
    maze_schema = schema
    print(f"Successfully grabbed MAZE Schema for table {i}...")
    
    print("Checking schema for differences\n")
    alter_smmcd3_table_schema(smmcd3_cursor,smmcd3_table_name,maze_schema)

    
    column_names = ', '.join(['"' + column.COLUMN_NAME  + '"' for column in schema])
    first_column_name = list(column_names.split(","))[0]
    
    smmcd3_cursor.execute(f"SELECT * FROM {i + '_Maze'}")
    smmcd3_data = smmcd3_cursor.fetchall()
    column_names_smmcd3 = [desc[0] for desc in smmcd3_cursor.description]
    smmcd3_cursor.commit()
    
    maze_cursor.execute(f"SELECT * FROM {i}")
    maze_data = maze_cursor.fetchall()
    column_names_maze = [desc[0] for desc in maze_cursor.description]
    maze_cursor.commit()

    #DELETE 
    print("Processing Deletes")
    for row in smmcd3_data:
        if row not in maze_data:
            
            first_value = str(row[0])
            first_value = "'" + first_value + "'"
            
            try:
                maze_cursor.execute(f"SELECT * FROM {i} WHERE {first_column_name} = {first_value}")
                check_primary_key = maze_cursor.fetchall()
                maze_cursor.commit()
                        
                if len(check_primary_key) == 0:
                    delete_query = f"DELETE FROM {i + '_Maze'} WHERE {first_column_name} = {first_value}"
                    smmcd3_cursor.execute(delete_query)
                    smmcd3_cursor.commit()
                    with open(f"./ExecutionLogs/{date.today()}-log.txt", '+a') as file:
                        file.write(f"[{datetime.now()}]: DELETE: {delete_query}\n")
                    file.close()
            
            except Exception as e:
                with open(f"./ExecutionLogs/{date.today()}-ExceptionLog.txt", '+a') as file:
                    file.write(f"[{datetime.now()}]: {str(e)} , Query: SELECT * FROM {i + '_Maze'} WHERE {first_column_name} = {first_value}\n")
                file.close()
                
    print("Refreshing Cursor for Insert")  
    maze_cursor.close()
    maze.close()
    smmcd3_cursor.close()
    smmcd3.close()    
    maze        = pyodbc.connect(maze_db)
    smmcd3      =  pyodbc.connect(smmcd3_db)
    maze_cursor = maze.cursor()
    smmcd3_cursor = smmcd3.cursor()

    #INSERT
    print("Processing Inserts")
    for row in maze_data:
        if row not in smmcd3_data:
            
            insert_query = f'INSERT INTO {i + "_Maze"} ({column_names}, "ValidFrom", "ValidTo") VALUES ('
            
            values = []
            first_value = str(row[0])
            
            for value in row:
                if isinstance(value, datetime):
                    formatted_value = str(value.date())
                    values.append(f"{formatted_value}")
                elif isinstance(value, str):
                    if len(value) > 255:
                        values.append("0")
                    else:
                        values.append(f"'{value}'")
                elif value is None:
                    values.append('NULL')
                elif isinstance(value, bool):
                    values.append('1' if value else '0')
                else:
                    values.append(str(value))
                    
            values = ", ".join(values)
            insert_query += f'{values}, DEFAULT, DEFAULT)'
            
            try:
                first_value = "'" + first_value + "'"     
                smmcd3_cursor.execute(f"SELECT * FROM {i + '_Maze'} WHERE {first_column_name} = {first_value}")
                check_primary_key = smmcd3_cursor.fetchall()
                smmcd3_cursor.commit()
                
                if len(check_primary_key) != 1:   
                    smmcd3_cursor.execute(insert_query)
                    smmcd3_cursor.commit()
                    with open(f"./ExecutionLogs/{date.today()}-log.txt", '+a') as file:
                        file.write(f"[{datetime.now()}]: INSERT: {insert_query}\n")
                    file.close()
            
            except Exception as e:
                with open(f"./ExecutionLogs/{date.today()}-ExceptionLog.txt", '+a') as file:
                    file.write(f"[{datetime.now()}]: {str(e)} , Query: SELECT * FROM {i + '_Maze'} WHERE {first_column_name} = {first_value}\n")
                file.close()

    print("Refreshing Cursor for Update")
    maze_cursor.close()
    maze.close()
    smmcd3_cursor.close()
    smmcd3.close()    
    maze        = pyodbc.connect(maze_db)
    smmcd3      =  pyodbc.connect(smmcd3_db)
    maze_cursor = maze.cursor()
    smmcd3_cursor = smmcd3.cursor()

    #UPDATE
    print("Processing Updates")
    for row_maze in maze_data:
        for row_smmcd3 in smmcd3_data:
            if str(row_maze[0]) == str(row_smmcd3[0]):
                if row_maze != row_smmcd3:
                    update_query = f"UPDATE {i + '_Maze'} SET "
                    columns_to_update = []
                                    
                    maze_dict = {column: value for column, value in zip(column_names_maze, row_maze)}
                    smmcd3_dict = {column: value for column, value in zip(column_names_smmcd3, row_smmcd3)}
                    
                    try:
                        for column in column_names_maze:
                            if str(maze_dict[column]) != str(smmcd3_dict[column]):
                                columns_to_update.append(f"{column} = '{maze_dict[column]}'")
                        
                        if columns_to_update:
                            update_query += ", ".join(columns_to_update)
                            update_query += f" WHERE {first_column_name} = {row_maze[0]}"
                            smmcd3_cursor.execute(update_query)
                            smmcd3_cursor.commit()
                            with open(f"./ExecutionLogs/{date.today()}-log.txt", '+a') as file:
                                file.write(f"[{datetime.now()}]: UPDATE: {update_query}\n")
                            file.close()
                    except Exception as e:
                        with open(f"./ExecutionLogs/{date.today()}-ExceptionLog.txt", '+a') as file:
                            file.write(f"[{datetime.now()}]:  {str(e)} , UPDATE: {update_query}\n")
                        file.close() 
                    
    maze_cursor.close()
    maze.close()
    smmcd3_cursor.close()
    smmcd3.close()
    print("Resetting Connections")
    
       
 #####  ####### ######   #####     #     #                                                        ######                      
#     # #       #     # #     #    #     # #  ####  #####  ####  #####  #  ####    ##   #         #     #   ##   #####   ##   
#       #       #     #       #    #     # # #        #   #    # #    # # #    #  #  #  #         #     #  #  #    #    #  #  
#       #####   #     #  #####     ####### #  ####    #   #    # #    # # #      #    # #         #     # #    #   #   #    # 
#       #       #     #       #    #     # #      #   #   #    # #####  # #      ###### #         #     # ######   #   ###### 
#     # #       #     # #     #    #     # # #    #   #   #    # #   #  # #    # #    # #         #     # #    #   #   #    # 
 #####  ####### ######   #####     #     # #  ####    #    ####  #    # #  ####  #    # ######    ######  #    #   #   #    # 


#----- CED3 Import ------
tables_ced3 = ["v_Activity", "v_ActivityEnrolments", "v_Parent", "v_ParentAddresses", "v_ParentLanguages", "v_Staff", "v_Student", "v_StudentAccessRestrictions", "v_StudentAddresses", "v_StudentEmergencyContacts", "v_StudentLanguages", "v_Relationship", "v_Subjects"]
for i in tables_ced3:

    # ----- Connecting to Maze -----
    ced3_server         = "redacted"
    ced3_username       = "redacted"
    ced3_password       = "redacted"

    ced3_db             = 'DRIVER={SQL Server};SERVER='+ced3_server+';UID='+ced3_username+';PWD='+ ced3_password+';DATABASE=redacted'
    ced3                = pyodbc.connect(ced3_db)
    print("CED3 Connection: ", ced3)

    smmcd3_server   = "redacted"
    smmcd3_username = "redacted"
    smmcd3_password = "redacted"

    smmcd3_db   = 'DRIVER={SQL Server};SERVER='+smmcd3_server+';UID='+smmcd3_username+';PWD='+ smmcd3_password+';DATABASE=redacted'
    smmcd3      =  pyodbc.connect(smmcd3_db)
    print("SMMD3 Connection: ", smmcd3)
    
    # ---- Cursor Setup ----
    ced3_cursor = ced3.cursor()
    smmcd3_cursor = smmcd3.cursor()

    table_name = "'"+i+"'"
    smmcd3_table_name = "'"+i+"_ced3'"

    # Grab Schema from source table
    schema = ced3_cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.Columns WHERE TABLE_NAME = {table_name}").fetchall()
    ced3_schema = schema
    print(f"Successfully grabbed CED3 Schema for table {i}...")
    
    print("Checking schema for differences\n")
    alter_smmcd3_table_schema(smmcd3_cursor,smmcd3_table_name,ced3_schema)

    
    column_names = ', '.join(['"' + column.COLUMN_NAME  + '"' for column in schema])
    first_column_name = list(column_names.split(","))[0]
    
    smmcd3_cursor.execute(f"SELECT * FROM {i + '_ced3'}")
    smmcd3_data = smmcd3_cursor.fetchall()
    column_names_smmcd3 = [desc[0] for desc in smmcd3_cursor.description]
    smmcd3_cursor.commit()
    
    ced3_cursor.execute(f"SELECT * FROM {i}")
    ced3_data = ced3_cursor.fetchall()
    column_names_ced3 = [desc[0] for desc in ced3_cursor.description]
    ced3_cursor.commit()

    #DELETE 
    print("Processing Deletes")
    for row in smmcd3_data:
        if row not in ced3_data:
            
            first_value = str(row[0])
            first_value = "'" + first_value + "'"
            
            try:
                ced3_cursor.execute(f"SELECT * FROM {i} WHERE {first_column_name} = {first_value}")
                check_primary_key = ced3_cursor.fetchall()
                ced3_cursor.commit()
                        
                if len(check_primary_key) == 0:
                    delete_query = f"DELETE FROM {i + '_ced3'} WHERE {first_column_name} = {first_value}"
                    smmcd3_cursor.execute(delete_query)
                    smmcd3_cursor.commit()
                    with open(f"./ExecutionLogs/{date.today()}-log.txt", '+a') as file:
                        file.write(f"[{datetime.now()}]: DELETE: {delete_query}\n")
                    file.close()
            
            except Exception as e:
                with open(f"./ExecutionLogs/{date.today()}-ExceptionLog.txt", '+a') as file:
                    file.write(f"[{datetime.now()}]: {str(e)} , Query: SELECT * FROM {i + '_ced3'} WHERE {first_column_name} = {first_value}\n")
                file.close()

    #INSERT
    print("Processing Inserts")
    for row in ced3_data:
        if row not in smmcd3_data:
            
            insert_query = f'INSERT INTO {i + "_ced3"} ({column_names}, "ValidFrom", "ValidTo") VALUES ('
            
            values = []
            first_value = str(row[0])
            
            for value in row:
                if isinstance(value, datetime):
                    formatted_value = str(value.date())
                    values.append(f"{formatted_value}")
                elif isinstance(value, str):
                    if len(value) > 255:
                        values.append("0")
                    else:
                        values.append(f"'{value}'")
                elif value is None:
                    values.append('NULL')
                elif isinstance(value, bool):
                    values.append('1' if value else '0')
                else:
                    values.append(str(value))
                    
            values = ", ".join(values)
            insert_query += f'{values}, DEFAULT, DEFAULT)'
            
            try:
                first_value = "'" + first_value + "'"     
                smmcd3_cursor.execute(f"SELECT * FROM {i + '_ced3'} WHERE {first_column_name} = {first_value}")
                check_primary_key = smmcd3_cursor.fetchall()
                smmcd3_cursor.commit()
                
                if len(check_primary_key) != 1:   
                    smmcd3_cursor.execute(insert_query)
                    smmcd3_cursor.commit()
                    with open(f"./ExecutionLogs/{date.today()}-log.txt", '+a') as file:
                        file.write(f"[{datetime.now()}]: INSERT: {insert_query}\n")
                    file.close()
            
            except Exception as e:
                with open(f"./ExecutionLogs/{date.today()}-ExceptionLog.txt", '+a') as file:
                    file.write(f"[{datetime.now()}]: {str(e)} , Query: SELECT * FROM {i + '_ced3'} WHERE {first_column_name} = {first_value}\n")
                file.close()
                
    #UPDATE
    print("Processing Updates")
    for row_ced3 in ced3_data:
        for row_smmcd3 in smmcd3_data:
            if str(row_ced3[0]) == str(row_smmcd3[0]):
                if row_ced3 != row_smmcd3:
                    update_query = f"UPDATE {i + '_ced3'} SET "
                    columns_to_update = []
                                    
                    ced3_dict = {column: value for column, value in zip(column_names_ced3, row_ced3)}
                    smmcd3_dict = {column: value for column, value in zip(column_names_smmcd3, row_smmcd3)}
                    
                    try:
                        for column in column_names_ced3:
                            if str(ced3_dict[column]) != str(smmcd3_dict[column]):
                                columns_to_update.append(f"{column} = '{ced3_dict[column]}'")
                        
                        if columns_to_update:
                            update_query += ", ".join(columns_to_update)
                            update_query += f" WHERE {first_column_name} = {row_ced3[0]}"
                            smmcd3_cursor.execute(update_query)
                            smmcd3_cursor.commit()
                            with open(f"./ExecutionLogs/{date.today()}-log.txt", '+a') as file:
                                file.write(f"[{datetime.now()}]: UPDATE: {update_query}\n")
                            file.close()
                    except Exception as e:
                        with open(f"./ExecutionLogs/{date.today()}-ExceptionLog.txt", '+a') as file:
                            file.write(f"[{datetime.now()}]:  {str(e)} , UPDATE: {update_query}\n")
                        file.close() 
                       
    ced3_cursor.close()
    ced3.close()
    smmcd3_cursor.close()
    smmcd3.close()
    print("Resetting Connections")

# ---- Completed ----
finish = time.time()
print(f"All specified tables migrated, closing connections.\nTotal Time Elapsed: {finish - start}")