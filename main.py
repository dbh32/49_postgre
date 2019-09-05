import psycopg2

students_list = [
    {
        'id': 302,
        'name': 'Mary',
        'gpa': 9.28,
        'birth': '09/12/91'
    },
    {
        'id': 617,
        'name': 'Phil',
        'gpa': 8.98,
        'birth': '13/03/96'
    },
    {
        'id': 482,
        'name': 'John',
        'gpa': 8.61,
        'birth': '21/11/87'
    },
]

courses = [
    {'id': 100,
     'name': 'Programming'},
    {'id': 200,
     'name': 'Design'}
]


def re():  # начинаем сначала :)
    try:
        drop_tables()
    except:
        pass

    create_db()


def drop_tables():  # удаляет таблицы
    with psycopg2.connect(dbname='hw_db',
                          user='hw_user',
                          password='netology') as conn:
        with conn.cursor() as cur:
            cur.execute('''DROP TABLE STUDENT_COURSE''')
            cur.execute('''DROP TABLE STUDENT''')
            cur.execute('''DROP TABLE COURSE''')

            conn.commit()


def create_db():  # создает таблицы, наполняет таблицу курсов
    with psycopg2.connect(dbname='hw_db',
                          user='hw_user',
                          password='netology') as conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE STUDENT  
                 (ID INTEGER NOT NULL PRIMARY KEY,
                 NAME CHARACTER VARYING(100) NOT NULL,
                 GPA NUMERIC(10,2),
                 BIRTH TIMESTAMP WITH TIME ZONE)
                 ;''')
            print('==> Student table created', '\n')

            cur.execute('''CREATE TABLE COURSE  
                 (ID INTEGER NOT NULL PRIMARY KEY,
                 NAME CHARACTER VARYING(100) NOT NULL)
                 ;''')
            print('==> Course table created', '\n')

            for course in courses:
                cur.execute('''INSERT INTO COURSE (ID, NAME) VALUES (%s, %s)''',
                            (course['id'], course['name']))
            print('==> Course table filled with values', '\n')

            cur.execute('''CREATE TABLE STUDENT_COURSE
                 (ID SERIAL PRIMARY KEY,
                 STUDENT_ID INTEGER REFERENCES STUDENT(ID),
                 COURSE_ID INTEGER REFERENCES COURSE(ID))
                 ;''')
            print('==> Student_course table created', '\n')

            conn.commit()


def get_students(course_id):  # возвращает студентов определенного курса
    with psycopg2.connect(dbname='hw_db',
                          user='hw_user',
                          password='netology') as conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT S.ID, S.NAME, C.NAME FROM STUDENT_COURSE SC 
            JOIN STUDENT S ON S.ID = SC.STUDENT_ID
            JOIN COURSE C ON C.ID = SC.COURSE_ID WHERE COURSE_ID=%s
            ''', (course_id,))
            rows = cur.fetchall()
            if len(rows) > 0:
                print('==> {} course students: '.format(rows[0][2]))
                for row in rows:
                    print('ID:', row[0])
                    print('Name:', row[1], '\n')
            else:
                print('==> No students on this course')
            conn.commit()


def add_students(course_id, students):  # создает студентов и записывает их на выбранный курс
    with psycopg2.connect(dbname='hw_db',
                          user='hw_user',
                          password='netology') as conn:
        with conn.cursor() as cur:
            for stud in students:
                cur.execute('''INSERT INTO STUDENT (ID, NAME, GPA, BIRTH) VALUES (%s, %s, %s, %s)''',
                            (stud['id'], stud['name'], stud['gpa'], stud['birth']))
                cur.execute('''INSERT INTO STUDENT_COURSE (STUDENT_ID, COURSE_ID) VALUES(%s, %s)''',
                            (stud['id'], course_id,))


def add_student(student):  # просто создает студента
    with psycopg2.connect(dbname='hw_db',
                          user='hw_user',
                          password='netology') as conn:
        with conn.cursor() as cur:
            cur.execute('''INSERT INTO STUDENT (ID, NAME, GPA, BIRTH) VALUES (%s, %s, %s, %s)''',
                        (student['id'], student['name'], student['gpa'], student['birth']))
            print('==> Student added', '\n')
            conn.commit()


def get_student(student_id):
    with psycopg2.connect(dbname='hw_db',
                          user='hw_user',
                          password='netology') as conn:
        with conn.cursor() as cur:
            cur.execute('''SELECT ID, NAME, GPA, BIRTH FROM STUDENT WHERE ID=%s''', (student_id,))
            print('==> Student found:')
            rows = cur.fetchall()
            for row in rows:
                print('ID:', row[0])
                print('Name:', row[1])
                print('GPA:', row[2])
                print('Birth Date:', row[3], '\n')
            conn.commit()


if __name__ == '__main__':
    re()
    add_student(students_list[0])
    add_students(100, students_list[1:3])
    get_student(302)
    get_students(100)
