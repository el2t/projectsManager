
# Import necessary libraries
import os
import sqlite3
import json
from flask import Flask, render_template, request, jsonify

# Initialize Flask application
app = Flask(__name__)

# Define the paths for projects and databases
# تحديد مسار مجلد المشاريع وقواعد البيانات
PROJECTS_DIR = '../../../GoalMeterics/DetectionExtraction/projects/'
DB_DIR = '../../../GoalMeterics/DetectionExtraction/databases/'

# Ensure directories exist
if not os.path.exists(PROJECTS_DIR):
    os.makedirs(PROJECTS_DIR)
if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR)

# Helper function to get a database connection
def get_db_connection(db_name):
    """
    Creates a connection to a specific SQLite database.
    """
    db_path = os.path.join(DB_DIR, db_name)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# Helper function to load project data
def load_project_data(project_id):
    """
    Loads project data from a JSON file.
    """
    project_file_path = os.path.join(PROJECTS_DIR, f'{project_id}.json')
    if not os.path.exists(project_file_path):
        return None, "ملف المشروع غير موجود"
    
    with open(project_file_path, 'r', encoding='utf-8') as f:
        project_data = json.load(f)
    return project_data, None

# Main route for the application
@app.route('/')
def index():
    """
    Serves the main page with a list of available projects.
    """
    projects = []
    try:
        project_files = [f for f in os.listdir(PROJECTS_DIR) if f.endswith('.json')]
        for p_file in project_files:
            project_id = p_file.replace('.json', '')
            project_data, error = load_project_data(project_id)
            if project_data:
                project_info = {
                    "id": project_id,
                    "display_name": project_id,
                    "status": project_data.get("status", "unknown"),
                    "video_path": project_data.get("paths", {}).get("vid_path", ""),
                    "db_path": project_data.get("paths", {}).get("db_path", ""),
                    "total_frames": project_data.get("total_frames", 0),
                    "processed_frames": project_data.get("processed_frames", 0),
                    "progress": project_data.get("progress", 0)
                }
                projects.append(project_info)
    except Exception as e:
        print(f"Error loading projects: {str(e)}")
    return render_template('index.html', projects=projects)

# Route to get a list of tables in a specific project
@app.route('/get_tables', methods=['POST'])
def get_tables():
    """
    Returns a list of tables in the specified project's database.
    """
    project_id = request.json.get('project_id')
    project_data, error = load_project_data(project_id)
    if error:
        return jsonify({"error": error}), 404

    db_name = project_data.get("paths", {}).get("db_path", "")
    if not db_name:
        return jsonify({"error": "لم يتم العثور على قاعدة بيانات للمشروع"}), 400

    conn = None
    try:
        conn = get_db_connection(db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row['name'] for row in cursor.fetchall()]
        return jsonify(tables)
    except sqlite3.OperationalError as e:
        return jsonify({"error": f"خطأ في قاعدة البيانات: {e}"}), 500
    finally:
        if conn:
            conn.close()

# Route to get a list of columns in a specific table
@app.route('/get_columns', methods=['POST'])
def get_columns():
    """
    Returns a list of columns for a given table.
    """
    project_id = request.json.get('project_id')
    table_name = request.json.get('table_name')
    
    project_data, error = load_project_data(project_id)
    if error:
        return jsonify({"error": error}), 404
    
    db_name = project_data.get("paths", {}).get("db_path", "")
    if not db_name:
        return jsonify({"error": "لم يتم العثور على قاعدة بيانات للمشروع"}), 400

    conn = None
    try:
        conn = get_db_connection(db_name)
        cursor = conn.cursor()
        
        # Validate table name to prevent SQL injection
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        if not cursor.fetchone():
            return jsonify({"error": "الجدول غير موجود"}), 404
            
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [row['name'] for row in cursor.fetchall()]
        return jsonify(columns)
    except sqlite3.OperationalError as e:
        return jsonify({"error": f"خطأ في قاعدة البيانات: {e}"}), 500
    finally:
        if conn:
            conn.close()

# Route to get data from a specific table with search and pagination
@app.route('/get_data', methods=['POST'])
def get_data():
    """
    Returns data from a table with search, sort, and pagination capabilities.
    """
    project_id = request.json.get('project_id')
    table_name = request.json.get('table_name')
    search_column = request.json.get('search_column')
    search_text = request.json.get('search_text')
    search_operator = request.json.get('search_operator', '=')
    limit = request.json.get('limit', 100)
    offset = request.json.get('offset', 0)
    sort_by = request.json.get('sort_by')
    sort_order = request.json.get('sort_order', 'ASC')

    project_data, error = load_project_data(project_id)
    if error:
        return jsonify({"error": error}), 404
    
    db_name = project_data.get("paths", {}).get("db_path", "")
    if not db_name:
        return jsonify({"error": "لم يتم العثور على قاعدة بيانات للمشروع"}), 400

    conn = None
    try:
        conn = get_db_connection(db_name)
        cursor = conn.cursor()
        
        # Validate table and column names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        if not cursor.fetchone():
            return jsonify({"error": "الجدول غير موجود"}), 404
            
        valid_columns = [row['name'] for row in cursor.execute(f"PRAGMA table_info({table_name});").fetchall()]
        
        if search_column and search_column not in valid_columns:
            return jsonify({"error": "عمود البحث غير صالح"}), 400
        if sort_by and sort_by not in valid_columns:
            return jsonify({"error": "عمود الفرز غير صالح"}), 400

        # Build dynamic SQL query
        query = f"SELECT * FROM {table_name}"
        params = []
        
        if search_column and search_text:
            if search_operator.upper() == 'LIKE':
                query += f" WHERE `{search_column}` LIKE ?"
                params.append(f'%{search_text}%')
            else:
                query += f" WHERE `{search_column}` {search_operator} ?"
                params.append(search_text)
        
        if sort_by:
            query += f" ORDER BY `{sort_by}` {sort_order}"
            
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Get total record count for pagination
        count_query = f"SELECT COUNT(*) as total FROM {table_name}"
        count_params = []
        if search_column and search_text:
            if search_operator.upper() == 'LIKE':
                count_query += f" WHERE `{search_column}` LIKE ?"
            else:
                count_query += f" WHERE `{search_column}` {search_operator} ?"
            count_params.append(params[0])
                
        count_cursor = conn.cursor()
        count_cursor.execute(count_query, count_params)
            
        total_records = count_cursor.fetchone()['total']
        
        column_names = [description[0] for description in cursor.description]
        data = [dict(row) for row in rows]
        
        return jsonify({
            'columns': column_names, 
            'data': data,
            'total_records': total_records,
            'current_page': offset // limit + 1,
            'total_pages': (total_records + limit - 1) // limit
        })
    except sqlite3.OperationalError as e:
        return jsonify({"error": f"خطأ في قاعدة البيانات: {e}"}), 500
    finally:
        if conn:
            conn.close()

# Route to get data statistics
@app.route('/get_stats', methods=['POST'])
def get_stats():
    """
    Returns statistics about the data in a table's column.
    """
    project_id = request.json.get('project_id')
    table_name = request.json.get('table_name')
    stat_column = request.json.get('stat_column')
    
    project_data, error = load_project_data(project_id)
    if error:
        return jsonify({"error": error}), 404
    
    db_name = project_data.get("paths", {}).get("db_path", "")
    if not db_name:
        return jsonify({"error": "لم يتم العثور على قاعدة بيانات للمشروع"}), 400

    conn = None
    try:
        conn = get_db_connection(db_name)
        cursor = conn.cursor()
        
        # Validate table and column names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        if not cursor.fetchone():
            return jsonify({"error": "الجدول غير موجود"}), 404

        valid_columns = [row['name'] for row in cursor.execute(f"PRAGMA table_info({table_name});").fetchall()]
        if stat_column not in valid_columns:
            return jsonify({"error": "عمود الإحصائيات غير صالح"}), 400

        stats = {}
        
        # Get unique values and their counts
        cursor.execute(f"SELECT `{stat_column}`, COUNT(*) as count FROM {table_name} GROUP BY `{stat_column}` ORDER BY count DESC")
        value_counts = {row[stat_column]: row['count'] for row in cursor.fetchall()}
        stats['value_counts'] = value_counts
        
        # Get numeric statistics if the column is numeric
        try:
            cursor.execute(f"SELECT MIN(`{stat_column}`) as min, MAX(`{stat_column}`) as max, AVG(`{stat_column}`) as avg FROM {table_name}")
            numeric_stats = cursor.fetchone()
            stats['min'] = numeric_stats['min']
            stats['max'] = numeric_stats['max']
            stats['avg'] = numeric_stats['avg']
        except:
            pass # Ignore if the column is not numeric
    
        return jsonify(stats)
    except sqlite3.OperationalError as e:
        return jsonify({"error": f"خطأ في قاعدة البيانات: {e}"}), 500
    finally:
        if conn:
            conn.close()

# Route to get detailed project information
@app.route('/get_project_info', methods=['POST'])
def get_project_info():
    """
    Returns detailed information about the specified project.
    """
    project_id = request.json.get('project_id')
    project_data, error = load_project_data(project_id)
    if error:
        return jsonify({"error": error}), 404
    
    return jsonify(project_data)

# Run the application
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)
