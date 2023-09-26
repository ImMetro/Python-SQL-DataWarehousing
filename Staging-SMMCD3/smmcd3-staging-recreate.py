import pyodbc
import time
from datetime import date, datetime

# ---- Tables to Import -----
tables_maze =  [
    "AF", "AKB", "AKC", "AKCT", "AKD", "AKK", "AKL", "AKLOG", "AR", "ARF", "ARLOG", "CAZSYS_BATCH_ARF",
    "CAZSYS_BATCH_CRF", "CAZSYS_BATCH_DFF", "CAZSYS_BATCH_DRF", "CAZSYS_BATCH_GLF", "CAZSYS_BATCH_GLFPREV",
    "CAZSYS_BATCH_MSG", "CAZSYS_E01", "CAZSYS_E02", "CAZSYS_H01", "CAZSYS_H02", "CAZSYS_H03", "CAZSYS_H04",
    "CAZSYS_L01", "CAZSYS_L02", "CAZSYS_L03", "CAZSYS_M01", "CAZSYS_M02", "CAZSYS_M03", "CAZSYS_M04",
    "CAZSYS_M05", "CAZSYS_M06", "CAZSYS_M07", "CAZSYS_M08", "CAZSYS_P00", "CAZSYS_R01", "CAZSYS_R02",
    "CAZSYS_R03", "CAZSYS_R04", "CAZSYS_R05", "CAZSYS_R06", "CAZSYS_R07", "CAZSYS_R08", "CAZSYS_R09",
    "CAZSYS_S01", "CAZSYS_S02", "CAZSYS_S03", "CAZSYS_S04", "CAZSYS_S05", "CAZSYS_S06", "CAZSYS_S07",
    "CAZSYS_S08", "CAZSYS_S09", "CAZSYS_S10", "CAZSYS_S11", "CAZSYS_S12", "CAZSYS_U01", "CAZSYS_U02",
    "CAZSYS_U03", "CR", "CRF", "CRFTC", "DF", "DFB", "DFF", "DFNI", "DFVT", "DR", "DRF", "EF", "email",
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
tables_ced3 = ["v_Activity", "v_ActivityEnrolments", "v_Parent", "v_ParentAddresses", "v_ParentLanguages", "v_Staff", "v_Student", "v_StudentAccessRestrictions", "v_StudentAddresses", "v_StudentEmergencyContacts", "v_StudentLanguages", "v_Relationship", "v_Subjects"]
start = time.time()
print(f"{date.today()}: Starting migration...")

# ----- Maze Import -----

for i in tables_maze:

    # ----- Connecting to Maze -----
    maze_server      = "redacted"
    maze_username    = "redacted"
    maze_password    = "redacted"


    maze_db     = 'DRIVER={SQL Server};SERVER='+maze_server+';UID='+maze_username+';PWD='+ maze_password+';DATABASE=redacted'
    maze        = pyodbc.connect(maze_db)
    print("Maze Connection: ", maze)

    # ----- Connecting to SMMCD3 -----
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

    # ---- Piping Data from Maze into SMMCD3 ----
    if smmcd3_cursor.tables(table=i, tableType="TABLE").fetchone():
        #If table exists, wipe it
        smmcd3_cursor.execute(f"DROP TABLE {i}")
        smmcd3_cursor.commit()
        print(f"Deleted Table {i}, proceeding to create it....")

    # Grab Schema from source table
    schema = maze_cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.Columns WHERE TABLE_NAME = {table_name}").fetchall()
    print(f"Successfully grabbed Maze Schema for table {i}...")

    #Col3 and Col7 Refer to the Table Name and Data Type
    #Example ('MazeViews', 'dbo', 'SF', 'SFKEY', 1, None, 'NO', 'varchar', 4, 4, None, None, None, None, None, None, 'iso_1', None, None, 'Latin1_General_CI_AS', None, None, None)
    smmcd3_cursor.execute(f'CREATE TABLE {i} ({", ".join([f"{col[3]} VARCHAR(255)" if col[7] == "varchar" else f"{col[3]} {col[7]}" for col in schema])})')
    smmcd3_cursor.commit()
    print(f"Created {i} Table in Staging-SMMCD3...")

    try:
        #Insert Data into New Table
        maze_cursor.execute(f"SELECT * FROM {i}")
        print("Executing Migration Now, this is purely RAW data only....")
        data = maze_cursor.fetchall()
        smmcd3_cursor.executemany(f'INSERT INTO {i} VALUES (' + ', '.join(['?' for _ in range(len(schema))]) + ')', data)
        smmcd3_cursor.commit()
        end = time.time()
        print(f"{end - start}: Table {i} has been migrated")
    except:
        print("Empty table.. Continuing..")

    # --- Close Connections ---
    # This allows the timeout error to be negated
    maze_cursor.close()
    maze.close()
    smmcd3_cursor.close()
    smmcd3.close()
    print("Refreshing access tokens for the next table...")


#----- CED3 Import ------

for i in tables_ced3:
    # ----- Connecting to CED3 -----
    ced3_server         = "redacted"
    ced3_username       = "redacted"
    ced3_password       = "redacted"

    ced3_db             = 'DRIVER={SQL Server};SERVER='+ced3_server+';UID='+ced3_username+';PWD='+ ced3_password+';DATABASE=redacted'
    ced3                = pyodbc.connect(ced3_db)
    print("CED3 Connection: ", ced3)

    # ----- Connecting to SMMCD3 -----
    smmcd3_server   = "redacted"
    smmcd3_username = "redacted"
    smmcd3_password = "redacted"

    smmcd3_db   = 'DRIVER={SQL Server};SERVER='+smmcd3_server+';UID='+smmcd3_username+';PWD='+ smmcd3_password+';DATABASE=redacted'
    smmcd3      =  pyodbc.connect(smmcd3_db)
    print("SMMD3 Connection: ", smmcd3)
    with open(f'./ExecutionLogs/{date.today()}-log.txt','a+') as file:
        file.write(f"SMMCD3 Connection: {smmcd3}")
    file.close

    # ---- Cursor Setup ----

    ced3_cursor = ced3.cursor()
    smmcd3_cursor = smmcd3.cursor()

    table_name = "'"+i+"'"

    # ---- Piping Data from CED3 into SMMCD3 ----
    if smmcd3_cursor.tables(table=i+"_ced3", tableType="TABLE").fetchone():
        #If table exists, wipe it
        smmcd3_cursor.execute(f"DROP TABLE {i+'_ced3'}")
        smmcd3_cursor.commit()
        print(f"Deleted Table {i}, proceeding to create it....")
        with open(f'./ExecutionLogs/{date.today()}-log.txt','a+') as file:
            file.write(f"Deleted Table {i}, proceeding to create it....")
        file.close

    # Grab Schema from source table
    schema = ced3_cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.Columns WHERE TABLE_NAME = {table_name}").fetchall()
    print(f"Successfully grabbed CED3 Schema for table {i}...")
    with open(f'./ExecutionLogs/{date.today()}-log.txt','a+') as file:
        file.write(f"Successfully grabbed CED3 Schema for table {i}...")
    file.close
    
    #Col3 and Col7 Refer to the Table Name and Data Type
    #Example ('MazeViews', 'dbo', 'SF', 'SFKEY', 1, None, 'NO', 'varchar', 4, 4, None, None, None, None, None, None, 'iso_1', None, None, 'Latin1_General_CI_AS', None, None, None)
    smmcd3_cursor.execute(f'CREATE TABLE {i + "_ced3"} ({", ".join([f"{col[3]} VARCHAR(255)" for col in schema])})')# if col[7] == "varchar" else f"{col[3]} {col[7]}" for col in schema])})')
    smmcd3_cursor.commit()
    print(f"Created {i} Table in Staging-SMMCD3...")
    with open(f'./ExecutionLogs/{date.today()}-log.txt','a+') as file:
        file.write(f"Created {i} Table in Staging-SMMCD3...")
    file.close

    #Insert Data into New Table
    ced3_cursor.execute(f"SELECT * FROM {i}")
    print("Executing Migration Now, this is purely RAW data only....")
    with open(f'./ExecutionLogs/{date.today()}-log.txt','a+') as file:
        file.write("Executing Migration Now, this is purely RAW data only....")
    file.close
    data = ced3_cursor.fetchall()
    smmcd3_cursor.executemany(f'INSERT INTO {i + "_ced3"} VALUES (' + ', '.join(['?' for _ in range(len(schema))]) + ')', data)
    smmcd3_cursor.commit()
    end = time.time()
    print(f"{end - start}: Table {i} has been migrated")
    with open(f'./ExecutionLogs/{date.today()}-log.txt','a+') as file:
        file.write(f"{end - start}: Table {i} has been migrated")
    file.close

    # --- Close Connections ---
    # This allows the timeout error to be negated
    ced3_cursor.close()
    ced3.close()
    smmcd3_cursor.close()
    smmcd3.close()
    print("Refreshing access tokens for the next table...")
    with open(f'./ExecutionLogs/{date.today()}-log.txt','a+') as file:
        file.write("Refreshing access tokens for the next table...")
    file.close   

# ---- Completed ----
finish = time.time()
print(f"All specified tables migrated, closing connections.\nTotal Time Elapsed: {finish - start}")