#!/usr/bin/env python
# coding: utf-8


from dotenv import load_dotenv
from pathlib import Path
import os
import psycopg2
import cx_Oracle
from sshtunnel import SSHTunnelForwarder

class Connexions:
    def __init__(self, server):
        dotenv_path = Path("C:/Users/des_eri/venv/Scripts/credentials.env")
        load_dotenv(dotenv_path = dotenv_path)
        self.server = server
    
    def infocentre(self):
        con = psycopg2.connect(user = os.getenv('infocentre_user'), 
                                  password = os.getenv('infocentre_password'), 
                                  host = os.getenv('infocentre_host'),
                                  port = os.getenv('infocentre_port'),
                                  database = os.getenv('infocentre_database'))
        cur = con.cursor()
        return con, cur

    def crm_optic2000(self):
        con = psycopg2.connect(user = os.getenv('crm_mark_user'), 
                                  password = os.getenv('crm_mark_password'), 
                                  host = os.getenv('crm_mark_host'),
                                  port = os.getenv('crm_mark_port'),
                                  database = os.getenv('crm_mark_database'))
        cur = con.cursor()
        return con, cur

    def datamart_reseau(self):
        con = psycopg2.connect(user = os.getenv('datamart_reseau_user'), 
                                  password = os.getenv('datamart_reseau_password'), 
                                  host = os.getenv('datamart_reseau_host'),
                                  port = os.getenv('datamart_reseau_port'),
                                  database = os.getenv('datamart_reseau_database'))
        cur = con.cursor()
        return con, cur


    def datamart_marketing(self):
        con = psycopg2.connect(user = os.getenv('datamart_mark_user'), 
                                  password = os.getenv('datamart_mark_password'), 
                                  host = os.getenv('datamart_mark_host'),
                                  port = os.getenv('datamart_mark_port'),
                                  database = os.getenv('datamart_mark_database'))
        cur = con.cursor()
        return con, cur
    
    def diapason(self):
        try:
            cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\product\11.2.0\client\BIN")
        except:
            pass
        
        con = cx_Oracle.connect(os.getenv('diapason_user'),
                                 os.getenv('diapason_password'), 
                                 os.getenv('diapason_host_port_database'))
        cur = con.cursor()
        
        return con, cur
    
    def prisme(self):
        try:
            cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\product\11.2.0\client\BIN")
        except:
            pass
        con = cx_Oracle.connect(os.getenv('prisme_credential'))
        cur = con.cursor()
        return con, cur

    def geo_re7(self):
        try:

            server_resept = SSHTunnelForwarder(
                                        (os.getenv('geo_resept_host_ssh'),int(os.getenv('geo_resept_port_ssh'))),
                                        ssh_username=os.getenv('geo_resept_user'),
                                        ssh_password=os.getenv('geo_resept_pwd'), 
                                        remote_bind_address=(os.getenv('geo_resept_host'),int(os.getenv('geo_resept_port')))
                                    )  
            
            server_resept.start()
            print ("server connected")

            conn = psycopg2.connect(
                                    database=os.getenv('geo_resept_database'),
                                    user=os.getenv('geo_resept_user'),
                                    host=os.getenv('geo_resept_host'),
                                    port=server_resept.local_bind_port,
                                    password=os.getenv('geo_resept_pwd')
                                )

            cur = conn.cursor()
            print ("database connected")

        except:
            print ("Connection Failed")
        return conn, cur

    def geo_preprod(self):
        try:

            server_preprod = SSHTunnelForwarder(
                                        (os.getenv('geo_preprod_host_ssh'),int(os.getenv('geo_preprod_port_ssh'))),
                                        ssh_username=os.getenv('geo_preprod_user'),
                                        ssh_password=os.getenv('geo_preprod_pwd'), 
                                        remote_bind_address=(os.getenv('geo_preprod_host'),int(os.getenv('geo_preprod_port')))
                                    )  
            
            server_preprod.start()
            print ("server connected")

            conn_preprod = psycopg2.connect(
                                    database=os.getenv('geo_preprod_database'),
                                    user=os.getenv('geo_preprod_user'),
                                    host=os.getenv('geo_preprod_host'),
                                    port=server_preprod.local_bind_port,
                                    password=os.getenv('geo_preprod_pwd')
                                )

            cur_preprod = conn_preprod.cursor()
            print ("database connected")

        except:
            print ("Connection Failed")
        return conn_preprod, cur_preprod

    def geo_prod(self):
        try:

            server_prod = SSHTunnelForwarder(
                                        (os.getenv('geo_prod_host_ssh'),int(os.getenv('geo_prod_port_ssh'))),
                                        ssh_username=os.getenv('geo_prod_user'),
                                        ssh_password=os.getenv('geo_prod_pwd'), 
                                        remote_bind_address=(os.getenv('geo_prod_host'),int(os.getenv('geo_prod_port')))
                                    )  
            
            server_prod.start()
            print ("server connected")

            conn_prod = psycopg2.connect(
                                    database=os.getenv('geo_prod_database'),
                                    user=os.getenv('geo_prod_user'),
                                    host=os.getenv('geo_prod_host'),
                                    port=server_prod.local_bind_port,
                                    password=os.getenv('geo_prod_pwd')
                                )

            cur_prod = conn_prod.cursor()
            print ("database connected")

        except:
            print ("Connection Failed")
        return conn_prod, cur_prod


    def local_postgres(self):
        con = psycopg2.connect(user = os.getenv('local_user'), 
                                  password = os.getenv('local_password'), 
                                  host = os.getenv('local_host'),
                                  port = os.getenv('local_port'),
                                  database = os.getenv('local_database'))
        cur = con.cursor()
        return con, cur

    def run(self):

        if self.server == "diapason":
            conn,cur = self.diapason()
        elif self.server == "prisme":
            conn, cur = self.prisme()
        elif self.server == "infocentre":
            conn, cur = self.infocentre()
        elif self.server == "geo_re7":
            conn, cur = self.geo_re7()
        elif self.server == "geo_preprod":
            conn, cur = self.geo_preprod()
        elif self.server == "geo_prod":
            conn, cur = self.geo_prod()
        elif self.server == "datamart_marketing":
            conn, cur = self.datamart_marketing()
        elif self.server == "datamart_reseau":
            conn, cur = self.datamart_reseau()
        elif self.server == "crm_optic2000":
            conn, cur = self.crm_optic2000()
        elif self.server == "local_postgres":
            conn, cur = self.local_postgres()
        return conn, cur

