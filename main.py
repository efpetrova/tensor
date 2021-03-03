import json
import psycopg2, psycopg2.extras

# Change accordingly
pg_url = "dbname=kate user=kate"

def create_table_if_not_exists(conn):
    """
    Run DDL to ensure that target table exists.
    :param conn: Postgres connection
    :return: None
    """
    with conn.cursor() as cursor:
        cursor.execute(
            """create table if not exists department (id integer PRIMARY KEY, parent_id integer, name text not null, 
                type char(2) not null)""")

def insert_into_table_if_empty(conn):
    """
    Load data.json file into database if empty

    :param conn: Postgres connection
    :return: None
    """
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 FROM department")
        exists = cursor.fetchone()
        if not exists:
            with open('data.json') as json_file:
                data = json.load(json_file)
                query = """INSERT into department (id, parent_id, name, type)
                               VALUES (%(id)s, %(ParentId)s, %(Name)s, %(Type)s)"""
                cursor.executemany(query, data)
                conn.commit()

def find_office(conn, employee_id):
    """
    Find office id and name by employee

    :param conn: Postgres connection
    :param employee_id: Int, Employee ID to find
    :return: tuple(office_id Int, office_name String)
    """
    with conn.cursor() as cursor:
        cursor.execute(f"""with recursive parent as (
                                 select parent_id, name,id 
                                   from department where id={employee_id}
                                  union all
                                 select department.parent_id, department.name,department.id
                                   from department 
                                   join parent on  parent.parent_id = department.id)
                               select id, name from parent where parent_id is null """)
        db_row = cursor.fetchone()
    return int(db_row[0]), db_row[1]

def find_all_employees_by_office_id(conn, office_id):
    """
    Find all employees names in the given office

    :param conn:  Postgres connection
    :param office_id: Int, office id
    :return: List[String], Employees names
    """
    with conn.cursor() as cursor:
        cursor.execute(f"""WITH RECURSIVE departments AS
                             (SELECT id, parent_id, 0 AS level, name,type 
                                FROM department WHERE parent_id IS NULL and id={office_id}
                              UNION ALL
                              SELECT child.id, child.parent_id, level+1, child.name,child.type 
                                FROM department child INNER JOIN departments p ON p.id=child.parent_id)
                              SELECT name
                                FROM departments
                               where departments.type='3'""")
        db_row = cursor.fetchall()
    return [str(x[0]) for x in db_row]


def run():
    """
    Run the whole use case scenario:
    1. Create tables if not exists
    2. Insert data if not exists
    3. Ask for the employee id to find
    4. Find office by employee
    5. Find all employees in the given office
    6. Display information on STDOUT

    :return: None
    """
    print('Enter id of employee:')
    employee_id = input()
    with psycopg2.connect(pg_url) as conn:
        create_table_if_not_exists(conn)
        insert_into_table_if_empty(conn)
        office_id, office_name = find_office(conn, employee_id)
        employees = find_all_employees_by_office_id(conn, office_id)
        message = f"{office_name}: {', '.join(employees)}"
        print(message)

if __name__ == "__main__":
    # execute only if run as a script
    run()
