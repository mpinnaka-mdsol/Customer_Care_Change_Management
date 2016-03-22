--**********************************
-- Author: Jane Goldiner
-- Date:   19 Nov 2013
-- Updated Murali Pinnaka
-- Updated 21 March 2016
-- URL:    bayer-bsp.mdsol.com
-- Rave Version: 2015.2.2
-- Description:
-- Set Edit CheckID to NULL in markings table where the edit check does not exist.
--**********************************

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects 
            WHERE id = Object_ID(N'tempdb.dbo.#Markings') AND type = 'U')
    drop table #Markings

IF EXISTS (SELECT * FROM sysobjects 
            WHERE id = Object_ID(N'tempdb.dbo.#Queries_TMP') AND type = 'U')
    drop table #Queries_TMP
    
IF EXISTS (SELECT * FROM sysobjects 
            WHERE id = Object_ID(N'dbo.#QueryNoChecks') AND type = 'U')
    drop table #QueryNoChecks
Go
-----------------------------------------------------------------------------------------------------------
declare @dt datetime,
        @DataPointID int,
        @error_number int, 
        @error_message nvarchar(2000),
        @BK_created bit,
        @StudyTypeID int,
        @StudySiteTypeID int,
        @SubjectTypeID int,
        @InstanceTypeID int,
        @DatapageTypeID int,
        @RecordTypeID int,
        @DataPointTypeID int,
        @ProjectName nvarchar(2000),
        @EnvironmentName nvarchar(2000)

select @StudyTypeID = ObjectTypeID from objecttyper where ObjectName = 'MedIData.Core.Objects.Study'
select @StudySiteTypeID = ObjectTypeID from objecttyper where ObjectName = 'MedIData.Core.Objects.StudySite'
select @SubjectTypeID = ObjectTypeID from objecttyper where ObjectName = 'MedIData.Core.Objects.Subject'
select @InstanceTypeID = ObjectTypeID from objecttyper where ObjectName = 'MedIData.Core.Objects.Instance'
select @DatapageTypeID = ObjectTypeID from objecttyper where ObjectName = 'MedIData.Core.Objects.DataPage'
select @RecordTypeID = ObjectTypeID from objecttyper where ObjectName = 'MedIData.Core.Objects.Record'
select @DatapointTypeID = ObjectTypeID from objecttyper where ObjectName = 'MedIData.Core.Objects.DataPoint'

set @error_number = 0
set @dt = getutcdate()
set @ProjectName = '13400'
set @EnvironmentName = 'DEV'

----------------- Create a backup table ------------------------

if not exists (select null from sys.objects where type = 'U' and name = N'BK_WR_2186290_Markings')
    begin
        create table BK_WR_2186290_Markings (DataPointID int, 
                                            MarkingID int, 
                                            EditCheckID int, 
                                            Updated datetime, 
                                            BK_timestamp datetime)

        set @BK_created = 1
    end
--------------------------------------------------------------
--  Select all markings, where there are edit check that don't exist in checks table,
--  but referenced in existing queries

select distinct dps.datapointID, markingID, MarkingTypeId, QueryStatusID, editcheckID, m.Updated, Text
into #QueryNoChecks
from markings m
left join checks c on c.checkID = m.editcheckID
inner join datapoints dps on dps.datapointid = m.datapointid
inner join subjects su on su.subjectid = dps.subjectid
inner join dbo.StudySites ss on ss.StudySiteID = su.StudySiteID
inner join dbo.Studies st on st.StudyID = ss.StudyID 
inner join dbo.Projects p on p.ProjectID = st.ProjectID 
where 1=1
and c.checkID is null
and dbo.fnlocaldefault(p.projectname) = @ProjectName
and dbo.fnlocaldefault(st.environmentnameid) = @EnvironmentName


-- affect only projects '13400' in dev environment
select	distinct dbo.fnLocalDefault(p.ProjectName) as ProjectName,
        dbo.fnLocalDefault(st.EnvironmentNameID) as Environment,
        dbo.fnLocalDefault(si.SiteNameID) as SiteName,
        s.SubjectID,
        s.SubjectName,
        dpg.DataPageID,
        dbo.fnLocalizedInstanceName('eng',i.ParentInstanceID) as ParentInstanceName,
        dbo.fnLocalizedInstanceName('eng',i.InstanceID) as InstanceName,
        dbo.fnLocalizedDataPageName('eng',dpg.DataPageID) as DataPageName,
        fi.FieldID,
        fi.OID as FieldOID,
        dbo.fnLocalDefault(fi.PreTextID) as Field,
        r.RecordID,
        r.RecordPosition, m.*
into #Markings
from dbo.DataPoints dp
inner join #QueryNoChecks m on m.datapointID = dp.datapointID
inner join dbo.Fields fi on fi.FieldID = dp.FieldID
inner join dbo.Records r on r.RecordID = dp.RecordID
inner join dbo.DataPages dpg on dpg.DataPageID = r.DataPageID
left join dbo.Instances i on i.InstanceID = dpg.InstanceID
inner join dbo.Subjects s on s.SubjectID = dpg.SubjectID
inner join dbo.StudySites ss on ss.StudySiteID = s.StudySiteID
inner join dbo.Sites si on si.SiteID = ss.SiteID
inner join dbo.Studies st on st.StudyID = ss.StudyID 
inner join dbo.Projects p on p.ProjectID = st.ProjectID 
where 1=1
and dbo.fnlocaldefault(p.projectname) = @ProjectName
and dbo.fnlocaldefault(st.environmentnameid) = @EnvironmentName


-------create table to select necessary objects to trigger the status rollup

SELECT distinct st.StudyID, 
                ss.StudySiteID, 
                s.SubjectID,  
                dpg.DataPageID, 
                r.RecordID, 
                dp.DataPointID
INTO #Queries_TMP
FROM dbo.Projects p
inner join dbo.Studies st on st.ProjectID = p.ProjectID
inner join dbo.StudySites ss on ss.StudyID = st.StudyID
inner join dbo.Subjects s on s.StudySiteID = ss.StudySiteID
inner join dbo.DataPages dpg on dpg.SubjectID = s.SubjectID
inner join dbo.Records r on r.DataPageID = dpg.DataPageID
left join dbo.DataPoints dp on dp.RecordID = r.RecordID 
WHERE s.SubjectId in (Select distinct SubjectID from #Markings)

select distinct i.InstanceID
into #Instances
from #Queries_TMP t
inner join dbo.Instances i on i.SubjectID = t.SubjectID



-------- update --------

begin transaction
begin try

-- Insert data into the backup table
    insert into BK_WR_2186290_Markings
    select distinct DataPointID, MarkingID, EditCheckID, Updated, @dt
    from #Markings 


---- Update Markings table to canceled queries opened on hidden datapoints 

        update dbo.Markings
        set	EditCheckID = NULL,
            Updated = @dt
        from #Markings t
        inner join Markings ms on ms.MarkingID = t.MarkingID

end try

begin catch
    select @error_number = error_number()
    select @error_message = error_message()
end catch

if @error_number = 0
   commit transaction
else
      begin
            rollback transaction
            print @error_message
      end
--------------------------------------------------------------------------
---- Update Status for the DataPoints by spSetDataPointHierarchyDirty ----

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #Queries_TMP t
inner join objectstatusallroles os on os.ObjectID = t.DataPointID and os.ObjectTypeID = @DatapointTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #Queries_TMP t
inner join objectstatusallroles os on os.ObjectID = t.RecordID and os.ObjectTypeID = @RecordTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #Queries_TMP t
inner join objectstatusallroles os on os.ObjectID = t.DataPageID and os.ObjectTypeID = @DatapageTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #Instances t
inner join objectstatusallroles os on os.ObjectID = t.InstanceID and os.ObjectTypeID = @InstanceTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #Queries_TMP t
inner join objectstatusallroles os on os.ObjectID = t.SubjectID and os.ObjectTypeID = @SubjectTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #Queries_TMP t
inner join objectstatusallroles os on os.ObjectID = t.StudySiteID and os.ObjectTypeID = @StudySiteTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #Queries_TMP t
inner join objectstatusallroles os on os.ObjectID = t.StudyID and os.ObjectTypeID = @StudyTypeID

update dbo.Records
set NeedsCVRefresh = 1
from #Queries_TMP t
inner join dbo.Records r on r.RecordID = t.RecordID


--------------------------------------------------------------------------- 
-- force status recalculations
---------------------------------------------------------------------------

SELECT distinct StudySiteID, -2 as RoleID, getUTCDate() as Dirty, studyID
into #StatusRollupQueue
from #Queries_TMP t

update t 
set t.RoleID = ro.roleID
from #StatusRollupQueue t
    JOIN userStudyRole usr
        on usr.studyID = t.studyID
    JOIN roles ro
        on ro.roleID = usr.roleID 
            and ro.active = 1
            and ro.roleID > 0

INSERT INTO StatusRollupQueue (ObjectTypeID, ObjectID, RoleID, Dirty)
SELECT distinct @StudySiteTypeID, StudySiteID, RoleID, getUTCDate()
FROM #StatusRollupQueue
        
---- results ----

Select * 
From #Markings
    

--- cleanup ---

IF EXISTS (SELECT * FROM tempdb.dbo.sysobjects 
            WHERE id = Object_ID(N'tempdb.dbo.#Markings') AND type = 'U')
    drop table #Markings

IF EXISTS (SELECT * FROM sysobjects 
            WHERE id = Object_ID(N'tempdb.dbo.#Queries_TMP') AND type = 'U')
    drop table #Queries_TMP
    
IF EXISTS (SELECT * FROM sysobjects 
            WHERE id = Object_ID(N'dbo.#QueryNoChecks') AND type = 'U')
    drop table #QueryNoChecks
    
IF EXISTS (SELECT * FROM sysobjects 
            WHERE id = Object_ID(N'dbo.#Instances') AND type = 'U')
    drop table #Instances
go
