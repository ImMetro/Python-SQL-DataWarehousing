from flask import Flask, render_template, request
import pyodbc
import webview

app = Flask(__name__)
window = webview.create_window('SMMC Info Lookup! by Peter Zhao', app)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/student_lookup')
def load_student():
    return render_template('student_lookup.html')

@app.route('/student_lookup', methods=['POST'])
def student_lookup():
    search_term = request.form['search']

    maze_server      = "redacted"
    maze_username    = "redacted"
    maze_password    = "redacted"
    
    maze_db     = 'DRIVER={SQL Server};SERVER='+maze_server+';UID='+maze_username+';PWD='+ maze_password+';DATABASE=redacted'
    
    smmcd3_server   = "redacted"
    smmcd3_username = "redacted"
    smmcd3_password = "redacted"

    smmcd3_db   = 'DRIVER={SQL Server};SERVER='+smmcd3_server+';UID='+smmcd3_username+';PWD='+ smmcd3_password+';DATABASE=redacted'
    smmcd3      =  pyodbc.connect(smmcd3_db) 
    
    # connect to the database, execute the query, and fetch the results
    connection = pyodbc.connect(maze_db)
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM ST WHERE FIRST_NAME LIKE '%{search_term}%'")
    results = cursor.fetchall()
    
    smmc_cursor = smmcd3.cursor()
    smmc_cursor.execute(f"SELECT * FROM [CENet-Login-ST] WHERE GivenName LIKE '%{search_term}%'")
    cenet = smmc_cursor.fetchall()
    
    combined_results = []
    for result in results:
        for cenett in cenet:
            if result.FIRST_NAME == cenett.GivenName and result.SURNAME == cenett.Surname:
                combined_results.append((result, cenett.NetworkLogin))
    
    connection.close()
    smmc_cursor.close()

    return render_template('student_lookup.html', search_term=search_term, combined_results=combined_results)

@app.route('/class_lookup')
def load():
    return render_template('class_lookup.html')

@app.route('/class_lookup', methods=['POST'])
def class_lookup():
    search_term = request.form['search']
    
    if search_term[-2:].isdigit():
        search_term = search_term[:-2] + '-' + search_term[-2:]
    elif search_term[-1].isdigit():
        search_term = search_term[:-1] + '-0' + search_term[-1:]
    else:
        search_term = search_term

    
    maze_server      = "redacted"
    maze_username    = "redacted"
    maze_password    = "redacted"
    
    maze_db     = 'DRIVER={SQL Server};SERVER='+maze_server+';UID='+maze_username+';PWD='+ maze_password+';DATABASE=redacted'
    
    smmcd3_server   = "redacted"
    smmcd3_username = "redacted"
    smmcd3_password = "redacted"

    smmcd3_db   = 'DRIVER={SQL Server};SERVER='+smmcd3_server+';UID='+smmcd3_username+';PWD='+ smmcd3_password+';DATABASE=redacted'
    smmcd3      =  pyodbc.connect(smmcd3_db) 
    
    # connect to the database, execute the query, and fetch the results
    connection = pyodbc.connect(maze_db)
    cursor = connection.cursor()
    query = f'''
    SELECT * FROM ST WHERE STKEY IN (
        SELECT StudentID FROM [v_sw_class_students] WHERE ClassID LIKE '%{search_term}%'
    )
    '''
    cursor.execute(query)
    results = cursor.fetchall()
    
    smmc_cursor = smmcd3.cursor()
    smmc_cursor.execute(f"SELECT * FROM [CENet-Login-ST]")# WHERE GivenName LIKE '%{search_term}%'")
    cenet = smmc_cursor.fetchall()
    
    combined_results = []
    for result in results:
        for cenett in cenet:
            if result.FIRST_NAME == cenett.GivenName and result.SURNAME == cenett.Surname:
                combined_results.append((result, cenett.NetworkLogin))
    
    connection.close()
    smmc_cursor.close()

    return render_template('class_lookup.html', search_term=search_term, combined_results=combined_results)

if __name__ == '__main__':
    # app.run(debug=True)
    webview.start()