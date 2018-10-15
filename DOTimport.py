#-------------------------------------------------------------------------------
# Name:        DOT Collission Data Import
# Purpose:     Download data from DOT, decrypt via pgp, insert into feature
#              class for mapping
#
# Author:      Randy Bonds Jr.
#
# Created:     25/08/2016
# Copyright:   (c) City of Yakima 2016
# Licence:     CCL 3.0 Attribution
#-------------------------------------------------------------------------------

import arcpy, csv, ftplib, datetime, gnupg, os
from arcpy import env

logpath = r"\path\to\your\logfile"
logfile = logpath + 'DOTmonthly.txt'
if arcpy.Exists(logfile):
    arcpy.Delete_management(logfile)

log = open(logfile, 'a')
print >> log, "---------------" + str(datetime.date.today()) + "---------------"

gpgbin = os.path.join("C:","\\Program Files (x86)","GNU","GnuPG","gpg2.exe")
gpgbin = "\"%s\"" % (gpgbin,) #embed space-containing path within literal double quotes
gpg = gnupg.GPG(gpgbinary=gpgbin)

month = datetime.datetime.now().strftime("%m-%Y") #use as filename
encryptedFilepath = "./downloads/"+ month +".ASC"
csvFilepath = "./downloads/"+ month +".csv"

#Open ftp connection and get the file
ftp = ftplib.FTP('ftp.wsdot.wa.gov', 'anonymous','')
ftp.cwd("/path/to/file")
gFile = open(encryptedFilepath, "wb")
ftp.retrbinary('RETR COLLI.ASC', gFile.write)
gFile.close()
ftp.quit()
if arcpy.Exists(encryptedFilepath):
    print >> log, "Downloaded " + encryptedFilepath

# decrypt file and write out csv
with open(encryptedFilepath, 'rb') as f:
    status = gpg.decrypt_file(f, passphrase='******', output=csvFilepath)

print >> log, 'ok: ', status.ok
print >> log, 'status: ', status.status
print >> log, 'stderr: ', status.stderr

#read csv file and import into feature class
if arcpy.Exists(csvFilepath):
    file = r''+csvFilepath
    reader = csv.reader(open(file, "rb"), delimiter = ",", skipinitialspace=True)

    #---Database Admin Connections---
    workspace = r"\path\to\yourfile.sde"
    sde = r"\path\to\your\db"
    edit = arcpy.da.Editor(workspace)
    edit.startEditing(False,True)
    arcpy.env.workspace = sde

    cursor = arcpy.da.InsertCursor( sde, ("Report_Number","Record_Type","Transaction_Type","City","County","Case_Number","Local_Agency_Coding","Collision_Date","Rural_Urban_Code","State_Functional_Class","Federal_Functional_Class","Fire_Resulted_Indicator","Stolen_Vehicle_Indicator","Hit_Run_Indicator","Number_of_Motor_Vehicles_Involv","Number_of_Pedestrians_Involved","Number_of_Pedalcyclists_Involve","Number_of_Injuries_Involved","Number_of_Fatalities_Involved","Accident_Severity","F1st_Collision_Type","F2nd_Collision_Type","F1st_Object_Struck","F2nd_Object_Struck","Junction_Relationship","Investigating_Agency","Roadway_Surface_Conditions","Weather_Conditions","Light_Conditions","Workzone_Status","Location_Character","Roadway_Character","Intentional_Action_Indicator","Medically_Caused_Indicator","Non_Traffic_Indicator","Legal_Intervention_Indicator","Police_Dispatched","Police_Arrived","Stateplane_X","Stateplane_Y","SHAPE@XY") )
    with open(file, 'rb') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None) #skip first line
        for row in reader:

            # only calculate point if there is a value
            xpoint = None
            ypoint = None
            if row[39] != '' and float(row[39]) > 0:
                xpoint = float(row[39])
                ypoint = float(row[40])

            #Change to zero if empty value
            if row[22] == '':
                row[22] = 31;
            if row[23] == '':
                row[23] = 0;
            if row[24] == '':
                row[24] = 0;
            if row[26] == '':
                row[26] = 0;
            if row[27] == '':
                row[27] = 0;
            if row[28] == '':
                row[28] = 0;
            if row[29] == '':
                row[29] = 0;
            if row[30] == '':
                row[30] = 0;
            if row[31] == '':
                row[31] = 99;
            if isinstance(row[31], basestring):
                row[31] = 99;
            if row[32] == '':
                row[32] = 9;
            if row[39] == '':
                row[39] = 0;
            if row[40] == '':
                row[40] = 0;
            #create new date time for sollision, combining fields
            collision_date = row[7]+' '+row[8]
            #print collision_date
            #print row
            #"A" Records are add, "D"  are remove.
            if row[2] != "D":
                print "Inserting Row";
                cursor.insertRow( [row[0],row[1],row[2],row[3],row[4],row[5],row[6],collision_date,row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25],row[26],row[27],row[28],row[29],row[30],row[31],row[32],row[33],row[34],row[35],row[36],row[37],row[38],row[39],row[40],(xpoint, ypoint)] )
            elif row[2] == "D":
                print "Deleting Row";
                print >> log, "delete" + row[0]
                remCursor = arcpy.UpdateCursor("\path\to\your\db", "Report_Number = '"+row[0]+"'")
                for remRow in remCursor:
                    remCursor.deleteRow(remRow)
                del remCursor
    del cursor
    # Stop the edit operation.
    #edit.stopOperation()

    # Stop the edit session and save the changes
    edit.stopEditing(True)
else:
    print >> log, "ERROR!! CSV File Doesn't Exist"
# Remove Duplicates
remCursor = arcpy.UpdateCursor("\path\to\your\db", "Report_Number In (SELECT Report_Number FROM YOURGEODB GROUP BY Report_Number HAVING Count(*)>1 ) AND Transaction_Type = 'A'")
for remRow in remCursor:
    remCursor.deleteRow(remRow)
del remCursor
print >> log, "Removed Duplicates";
log.close()