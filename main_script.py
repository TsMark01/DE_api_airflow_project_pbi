from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, VARCHAR, Date
from sqlalchemy.orm import declarative_base
from datetime import datetime
from functions import *

Base = declarative_base()
import argparse  # Импортируем библиотеку для обработки аргументов командной строки

parser = argparse.ArgumentParser()

parser.add_argument("--api_url", dest="api_url")
parser.add_argument("--api_key", dest="api_key")
parser.add_argument("--query_date", dest="query_date")  # Обработка параметра query_date
parser.add_argument("--host", dest="host")
parser.add_argument("--dbname", dest="dbname")
parser.add_argument("--user", dest="user")
parser.add_argument("--jdbc_password", dest="jdbc_password")
parser.add_argument("--port", dest="port")
args = parser.parse_args()  # Парсим аргументы командной строки и сохраняем их в переменной `args`

v_host = str(args.host)
v_dbname = str(args.dbname)
v_user = str(args.user)
v_password = str(args.jdbc_password)
v_port = str(args.port)

v_api_key = str(args.api_key)
v_api_url = str(args.api_url)
query_date = str(args.query_date)

query_date = datetime.strptime(query_date, "%Y-%m-%d").strftime("%d.%m.%Y")

print(v_api_url, v_api_key, query_date, v_host, v_dbname)

SQLALCHEMY_DATABASE_URI = f"postgresql://{v_user}:{v_password}@{v_host}:{v_port}/{v_dbname}"

print(SQLALCHEMY_DATABASE_URI)

# Создание подключения к базе данных
engine = create_engine(SQLALCHEMY_DATABASE_URI)

# Создание всех таблиц на основе метаданных базового класса
Base.metadata.create_all(bind=engine)

# Создание локальной сессии для взаимодействия с базой данных
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session_local = SessionLocal()


class Record(Base):
    __tablename__ = 'main_table_api_mis'  # Имя таблицы в базе данных

    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)  # Уникальный идентификатор
    patient_id = Column(Integer, nullable=True, index=True)  # Идентификатор счета
    company_type = Column(VARCHAR(length=100), nullable=True, index=True)  # Тип компании
    sum_value = Column(Integer, nullable=False, index=True)  # Сумма значения
    query_date = Column(Date, nullable=False, index=True)  # Дата запроса


# Функция для добавления новой записи в базу данных
def new_record_to_db_invoices(patient_id, company_type, sum_value, query_date):
    try:
        # Преобразуем query_date в формат YYYY-MM-DD
        query_date = datetime.strptime(query_date, "%d.%m.%Y").date()

        record = Record(  # Создаем новый объект записи
            patient_id=patient_id,
            company_type=company_type,
            sum_value=sum_value,
            query_date=query_date
        )

        session_local.add(record)  # Добавляем запись в локальную сессию
        session_local.commit()  # Сохраняем изменения в базе данных
    except Exception as e:
        print(f'Упс, ошибка - {e}\n'
              f'patient_id = {patient_id}\n'
              f'query_date = {query_date}')  # Выводим сообщение об ошибке


############################################################################################################################################################
############################################################################################################################################################


def get_patient_info(api_key, api_url, query_date):
    try:
        # Получаем назначения пациента
        appointments = get_patient_appointments(api_key, api_url, query_date)
        print(appointments)
        # Проверка на пустой массив
        if not appointments:
            print(f"Нет назначений на дату {query_date}. Записываем в БД сумму = 0.")
            # Записываем в БД только дату и сумму = 0
            new_record_to_db_invoices(patient_id=None, company_type="Нет назначений", sum_value=0, query_date=query_date)
            return  # Выходим из функции, так как нет назначений

        appointments_ids = [appointment['appointment_id'] for appointment in appointments]

        # Получаем уникальные invoice_id для назначений
        invoice_ids = getAppointmentServices(api_key, appointments_ids, api_url)

        # Получаем счета по invoice_id
        invoices = get_invoices(api_key, invoice_ids, api_url)

        # Изменяем на более понятное имя переменной
        invoice_info = [(invoice["patient_id"], invoice["company_id"], invoice["value"]) for invoice in invoices]

        for set_of_info in invoice_info:
            patient_id, company_id, sum_value = set_of_info
            company_details = get_company_types(api_key, company_id, query_date, api_url)
            company_type = company_details[0]["company_type"] if company_details else "Неизвестный тип"

            if company_type is not None:
                print(f"Дата: {query_date}, ID счета:   ID пациента: {patient_id}, ID компании: {company_id}, "
                      f"Тип: {company_type}, Сумма счета: {sum_value} руб.")

                new_record_to_db_invoices(patient_id=patient_id, company_type=company_type, sum_value=sum_value,
                                          query_date=query_date)

            else:
                company_type = "Физ.лицо"

                print(f"Дата: {query_date}, ID пациента: {patient_id}, ID компании: None, "
                      f"Тип: {company_type}, Сумма счета: {sum_value} руб.")
                new_record_to_db_invoices(patient_id=patient_id, company_type=company_type, sum_value=sum_value,
                                          query_date=query_date)

        print(f"Всего назначений: {len(appointments_ids)}")
        print(f"Всего счетов : {len(invoice_ids)}")

    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
        print("Ошибка произошла в функции get_patient_info.")

get_patient_info(api_key=v_api_key, api_url=v_api_url, query_date=query_date)

