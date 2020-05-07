# -*- coding:utf-8 -*-
import pandas as pd
import MySQLdb
import time
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf8')
# pandas以及MySQLdb需要安装
def backup():
        backupDate = time.strftime("%Y%m%d")
        print backupDate
# 打开数据库连接
        conn = MySQLdb.Connect(
            host='10.179.10.115',
            port=3306,
            user='zabbix',
            passwd='zabbix123',
            db='CSV-TO-DB',  # 数据库名称
            charset='utf8'
        )
        # 使用cursor()方法获取操作游标
        cursor = conn.cursor()
        #sql = """rename table PlayerInfo to %s"""
        cursor.execute("""CREATE TABLE IF NOT EXISTS PlayerInfo{} (ID int(255))""".format(backupDate))
        cursor.execute("""drop table PlayerInfo{}""".format(backupDate))
        cursor.execute("""CREATE TABLE PlayerInfo{} (ID int(255) primary key not null auto_increment,Hostname varchar(255),IP varchar(255),MAC varchar(255),last_update varchar(255),config_display_manager varchar(255),country_platform varchar(255),config_force_bsplayer_version varchar(255),available_updates varchar(255),xorg_driver_autoconfigured varchar(255),storage_type varchar(255),computer_manufacturer varchar(255),config_separate_network varchar(255),config_daily_poweroff varchar(255),graphic_adapter varchar(255),project_code_platform varchar(255),os_platform varchar(255),player varchar(255),environment varchar(255),bsplayer_infra varchar(255),platform_version varchar(255),computer_model varchar(255),bs_player_version varchar(255),regionalised varchar(255),offline varchar(255),config_postimport varchar(255),config_smartsync_smartcontent varchar(255),age varchar(255),uefi_supported varchar(255),update_now varchar(255),bios_is_up_to_date varchar(255),bios_version varchar(255),os_update_status varchar(255))""".format(backupDate))
        conn.commit()
        print ('backup success')
        cursor.close()
        # 关闭数据库连接
        conn.close()

# 从csv文件中读取数据，分别为：X列表和对应的Y列表
def get_data(file_name):
    backupDate = time.strftime("%Y%m%d")
    # 1. 用pandas读取csv
    df = pd.read_csv(file_name)
    data = df.astype(object).where(pd.notnull(df), None)
    # print (data)
    for Hostname,IP,MAC,last_update,config_display_manager,country_platform,config_force_bsplayer_version,available_updates,xorg_driver_autoconfigured,storage_type,computer_manufacturer,config_separate_network,config_daily_poweroff,graphic_adapter,project_code_platform,os_platform,player,environment,bsplayer_infra,platform_version,computer_model,bs_player_version,regionalised,offline,config_postimport,config_smartsync_smartcontent,age,uefi_supported,update_now,bios_is_up_to_date,bios_version,os_update_status in zip(data['Hostname'],data['IP'],data['MAC'],data['last_update'],data['config_display_manager'],data['country_platform'],data['config_force_bsplayer_version'],data['available_updates'],data['xorg_driver_autoconfigured'],data['storage_type'],data['computer_manufacturer'],data['config_separate_network'],data['config_daily_poweroff'],data['graphic_adapter'],data['project_code_platform'],data['os_platform'],data['player'],data['environment'],data['bsplayer_infra'],data['platform_version'],data['computer_model'],data['bs_player_version'],data['regionalised'],data['offline'],data['config_postimport'],data['config_smartsync_smartcontent'],data['age'],data['uefi_supported'],data['update_now'],data['bios_is_up_to_date'],data['bios_version'],data['os_update_status']):
        dataList = [Hostname,IP,MAC,last_update,config_display_manager,country_platform,config_force_bsplayer_version,available_updates,xorg_driver_autoconfigured,storage_type,computer_manufacturer,config_separate_network,config_daily_poweroff,graphic_adapter,project_code_platform,os_platform,player,environment,bsplayer_infra,platform_version,computer_model,bs_player_version,regionalised,offline,config_postimport,config_smartsync_smartcontent,age,uefi_supported,update_now,bios_is_up_to_date,bios_version,os_update_status]
        print (dataList)
        # 打开数据库连接
        conn = MySQLdb.Connect(
            host='10.179.10.115',
            port=3306,
            user='zabbix',
            passwd='zabbix123',
            db='CSV-TO-DB',  # 数据库名称
            charset='utf8'
        )
        # 使用cursor()方法获取操作游标
        cursor = conn.cursor()
        #df2 = df.astype(object).where(pd.notnull(df), None)
        try:
            insertsql = "INSERT INTO PlayerInfo{}(Hostname,IP,MAC,last_update,config_display_manager,country_platform,config_force_bsplayer_version,available_updates,xorg_driver_autoconfigured,storage_type,computer_manufacturer,config_separate_network,config_daily_poweroff,graphic_adapter,project_code_platform,os_platform,player,environment,bsplayer_infra,platform_version,computer_model,bs_player_version,regionalised,offline,config_postimport,config_smartsync_smartcontent,age,uefi_supported,update_now,bios_is_up_to_date,bios_version,os_update_status) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(insertsql.format(backupDate),dataList)
            conn.commit()
            print ('数据已成功插入')
        except Exception as e:
            print e
            conn.rollback()
        cursor.close()
        # 关闭数据库连接
        conn.close()

def arrangement():
        backupDate = time.strftime("%Y%m%d")
        # 打开数据库连接
        conn = MySQLdb.Connect(
            host='10.179.10.115',
            port=3306,
            user='zabbix',
            passwd='zabbix123',
            db='CSV-TO-DB',  # 数据库名称
            charset='utf8'
        )
        # 使用cursor()方法获取操作游标
        cursor = conn.cursor()
        cursor.execute("""DELETE FROM `PlayerInfo{}` WHERE last_update IN (
        SELECT * from (
                SELECT last_update FROM PlayerInfo{} WHERE Hostname
                IN (
                                SELECT hostname FROM `PlayerInfo{}` GROUP BY Hostname HAVING COUNT(Hostname) > 1
                ) AND last_update NOT IN (
                                SELECT MAX(last_update) from `PlayerInfo{}` GROUP BY Hostname HAVING COUNT(Hostname) > 1
                        )
        )as t
)""".format(backupDate,backupDate,backupDate,backupDate))
        cursor.execute("""DELETE FROM `PlayerInfo{}` WHERE IP is NULL and Hostname is NULL""".format(backupDate))
        conn.commit()
        print ('脏数据已清除')
        cursor.close()
        # 关闭数据库连接
        conn.close()

def main():
    #删除历史数据
    backup()
    # 读取数据 如果是别的格式的文件 可以转换成csv格式
    get_data('/home/jcdcn/entities.csv')
    arrangement()


if __name__ == '__main__':
    main()
