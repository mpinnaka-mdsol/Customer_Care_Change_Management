
--****************************************
-- Author:			Murali Pinnaka
-- Reviewed by: 
-- Date created:	28 Jan 2009
-- Date updated:	12 Mar 2014
-- WR:				2104038
-- URL:				bostonscientific-trails.mdsol.com 
-- ZERO_AF / Prod environment
-- Defect related: DT 10653 

-- Based on spMigrationFillInMissingDataPoints (author of the stored procedures is JCohen)

-- The script fills in missing datapoints in a record position 0 
-- where there are datapoints in recorn position greater then 0 
-- for the subject 'EN 1630ZA004/EN 1798ZA029' in 'AE' form 'in project 'ZERO_AF' in 'prod' environment
--***************************************
If exists (select NULL from sys.objects where name = 'cspFillInMissingDataPoints_WR2104038' and type = 'P')
	begin
		Drop procedure [dbo].[cspFillInMissingDataPoints_WR2104038]
		Print 'Dropping Procedure cspFillInMissingDataPoints_WR2104038'
	end
Go

PRINT 'Creating Procedure cspFillInMissingDataPoints_WR2104038'
GO

create Procedure cspFillInMissingDataPoints_WR2104038  @SubjectID int,  @DataPageID int, @Locale char(3)  
as  
set nocount on  
begin try  
 declare   @UtcDate datetime,   
		   @userID	int,  
		   @RecordID int,  
		   @FormID  int,  
		   @FieldID int,  
		   @VariableID int,  
		  -- @DataPageID int,  
		   @InstanceID int,  
		   @StudySiteID int,  
		   @StudyID int,  
		   @RecordActive bit,  
		   @RecordPosition int,  
		   @FormActive bit,  
		   @RecordTypeID int,  
		   @SubjectMatrixID int,  
		   @RepeatingDefaultDelimiter nchar(1),  -- this is by design in Rave, even though config value is nvarchar(2000), so will take the first character  
		   @MaxLogDefaults int,  
		   @DefaultValue nvarchar(2000),  
		   @IsUpperCaseConfig bit,
		   @guid char(36) 
 declare @DefaultValueList table (seq int identity(1, 1), Item nvarchar(2000))  
   
 select @IsUpperCaseConfig = case when exists (select null from Configuration where Tag = 'AllUpperCase' 
												and ConfigValue = 'True') then 1 else 0 end  
 select  
  @UtcDate = GetUtcDate(),   
  @userid = -2,   
  @StudySiteID = S.StudySiteID,   
  @StudyID = SS.StudyID  
 from DataPages DPG
 inner join Subjects S on S.SubjectID = dpg.SubjectID  
 inner join StudySites SS on S.StudySiteID = SS.StudySiteID 
 where DPG.SubjectID = @SubjectID  and DPG.DataPageID = @DataPageID
   
 select @RecordTypeID = ObjectTypeID from ObjectTypeR where ObjectName = 'Medidata.Core.Objects.Record'  
   
 select @RepeatingDefaultDelimiter = coalesce(Left(ConfigValue, 1), '|') from Configuration where Tag = 'RepeatingDefaultDelimiter'  
 if LTrim(@RepeatingDefaultDelimiter) = ''  
  select @RepeatingDefaultDelimiter = '|'  
   
 -- We are only interested in the log pages for the subject.  
 declare @DataPages table  
 (  
	  FormID int,  
	  FormActive bit,  
	  DataPageID int,  
	  InstanceID int,  
	  SubjectMatrixID int,  
	  MaxLogDefaults int  
 )  
 insert into @DataPages (FormID, FormActive, DataPageID, InstanceID, SubjectMatrixID, MaxLogDefaults)  
 select distinct M.FormID, M.FormActive, P.DataPageID, P.InstanceID, P.SubjectMatrixID, 1  
 from Forms M   
 inner join DataPages P on P.FormID = M.FormID and P.SubjectID = @SubjectID  and P.DataPageID = @DataPageID  
 inner join Fields F on M.FormID = F.FormID and F.IsLog = 1 and F.FieldActive = 1  
 declare @Fields table  
 (  
	  FieldID int,  
	  DefaultValue nvarchar(2000)  
 )  
 declare Form_Cursor cursor fast_forward for select distinct FormID from @DataPages  
 open Form_Cursor  
 fetch next from Form_Cursor into @FormID  
 while @@fetch_status = 0  
 begin  
  select @MaxLogDefaults = 1  
  insert into @Fields (FieldID, DefaultValue) 
  select FieldID, DefaultValue 
  from Fields 
  where FormID = @FormID and FieldActive = 1 and IsLog = 1 -- we only care about defaults for loglines when calculating max default. 
   
  if exists (select null from @Fields where CharIndex(@RepeatingDefaultDelimiter, DefaultValue) > 0)  
  begin  
   declare @CurrentDefaultCount int  
   declare Field_Cursor cursor fast_forward for  
    select FieldID, DefaultValue from @Fields where coalesce(DefaultValue, '') <> ''  
   open Field_Cursor
   fetch next from Field_Cursor into @FieldID, @DefaultValue  
   while @@fetch_status = 0  
   begin  
    select @CurrentDefaultCount = count(Item) from dbo.fnParseDelimitedString(@DefaultValue, @RepeatingDefaultDelimiter) where Item <> ''  
    if @CurrentDefaultCount > @MaxLogDefaults  
     select @MaxLogDefaults = @CurrentDefaultCount  
    fetch next from Field_Cursor into @FieldID, @DefaultValue  
   end  
   close Field_Cursor  
   deallocate Field_Cursor  
  end  
  delete from @Fields  
  if @MaxLogDefaults > 1  
   update @DataPages set MaxLogDefaults = @MaxLogDefaults where FormID = @FormID  
    
  fetch next from Form_Cursor into @FormID  
 end  
 close Form_Cursor  
 deallocate Form_Cursor  
   
 declare Form_Cursor cursor fast_forward for select * from @DataPages  
 open Form_Cursor  
 fetch next from Form_Cursor into @FormID, @FormActive, @DataPageID, @InstanceID, @SubjectMatrixID, @MaxLogDefaults  
 while @@fetch_status = 0  
 begin  
  declare @Counter int  
  select @Counter = count(*) from Records where DataPageID = @DataPageID and Deleted = 0  
  while @Counter < (@MaxLogDefaults + 1)  
  begin  
   select @RecordPosition = @Counter  
   insert into Records (RecordName, RecordActive, SubjectID, FormID, InstanceID, Created, Updated, RecordPosition, GUID, SubjectMatrixID, DataPageID, Deleted, NeedsCvRefresh)  
   values ('{FORMNAME} ({RECPOS})', @FormActive, @SubjectID, @FormID, @InstanceID, @UtcDate, @UtcDate, @RecordPosition, NewID(), @SubjectMatrixID, @DataPageID, 0, 1)  
   select @RecordID = scope_identity()  

   insert into Audits (ObjectID, ObjectTypeID, Property, Value, Readable, AuditUserID, AuditTime, AuditSubCategoryID)  
   values (@RecordID, @RecordTypeID, '', '', 'Record updated (WR 2104038)', @UserID, @UtcDate, 75)  
   select @Counter = @Counter + 1  
  end  
  if @Counter > (@MaxLogDefaults + 1)  
  begin  
   if not exists (  
    select null from DataPoints D  
     inner join Records R on D.RecordID = R.RecordID  
    where R.DataPageID = @DataPageID and D.IsTouched = 1)  
   begin  
    update Records set Deleted = 1, RecordActive = 0 where DataPageID = @DataPageID and RecordPosition > @MaxLogDefaults  
   end  
  end  
  fetch next from Form_Cursor into @FormID, @FormActive, @DataPageID, @InstanceID, @SubjectMatrixID, @MaxLogDefaults  
 end  
 close Form_Cursor  
 deallocate Form_Cursor  
 declare Record_Cursor cursor fast_forward for   
  select R.RecordID, R.FormID, R.DataPageID, P.InstanceID, R.RecordActive, R.RecordPosition  
  from Records R inner join DataPages P on R.DataPageID = P.DataPageID  
  where R.SubjectID = @SubjectID   
   and R.DataPageID = @DataPageID
--and exists (select null from DataPoints D where D.DataPageID = R.DataPageID and D.Deleted = 0)  
   and exists (select null from Fields F where F.FormID = R.FormID)  
   and R.Deleted = 0  
 open Record_Cursor  
 fetch next from Record_Cursor into @RecordID, @FormID, @DataPageID, @InstanceID, @RecordActive, @RecordPosition  
 while @@fetch_status = 0  
 begin  
  declare @IsVisible   bit,  
		@IsLog    bit,  
		@DerivationID  int,  
		@IsFrozen   bit,  
		@IsHidden   bit,  
		@Data    nvarchar(2000),  
		@DataDictID   int,  
		@DataDictEntryID int,  
		@DefaultCount  int,  
		@DataFormat   nvarchar(255)  
  declare Field_Cursor cursor fast_forward for   
   select F.FieldID,   
		F.VariableID,   
		F.IsVisible,   
		coalesce(F.IsLog, 0),   
		V.DerivationID,   
		case when V.DerivationID is null then 0 else 1 end as IsFrozen,  
		case   
		 when (@RecordPosition = 0 and coalesce(F.IsLog, 0) = 0) or  
		   (@RecordPosition > 0 and coalesce(F.IsLog, 0) = 1) then 0  
		 else 1  
		 end as IsHidden,  
		F.DefaultValue,  
		V.DataDictID,  
		V.DataFormat  
   from Fields F inner join Variables V on F.VariableID = V.VariableID  
   where F.FormID = @FormID   
   and not exists (select null from DataPoints where FieldID = F.FieldID and RecordID = @RecordID and Deleted = 0)  
   and FieldActive = 1  
  open Field_Cursor  
  fetch next from Field_Cursor into @FieldID, @VariableID, @IsVisible, @IsLog, @DerivationID, @IsFrozen, @IsHidden, @DefaultValue, @DataDictID, @DataFormat  
  while @@fetch_status = 0  
  begin  
   declare @DataPointID int, @tmpstr nvarchar(2000)  
   select @Data = '', @DataDictEntryID = 0  
   if coalesce(@DefaultValue, '') <> ''  
   begin  
    delete from @DefaultValueList  
    if @IsLog = 1  
    begin  
     insert into @DefaultValueList(Item)  select Item from dbo.fnParseDelimitedString(@DefaultValue, @RepeatingDefaultDelimiter)  
     select @DefaultCount = @@RowCount   
    end  
    if @IsLog = 0  
    begin  
     select @Data = @DefaultValue  
     select @DefaultCount = 1  
    end  
    else if @RecordPosition > 0 and @RecordPosition <= @DefaultCount  
    begin  
    select @Data = Item from (select seq, Item, row_number() over (order by seq) RowNum from @DefaultValueList) OrderedDefaultValueList   
      where RowNum = @RecordPosition  
     select @Data = coalesce(@Data, '')  
    end  
      
    if coalesce(@DataDictID, 0) > 0 and (@IsLog = 0 or @RecordPosition <= @DefaultCount) and @Data <> ''  
     select @DataDictEntryID = coalesce(DataDictionaryEntryID, 0) from DataDictionaryEntries where DataDictionaryID = @DataDictID and CodedData = @Data  
    else  
     select @DataDictEntryID = 0  
   end  
   if @IsUpperCaseConfig = 1  
    select @Data = Upper(@Data)  
select @guid = NewID(), @DataDictEntryID = case when @DataDictEntryID = 0 then NULL else @DataDictEntryID end
EXEC [spDataPointInsert]    
 @RecordID = @RecordID, @VariableID = @VariableID, @Data = @Data,    
 @AltCodedData = NULL, @DataDictEntryID = @DataDictEntryID, @UnitDictEntryID = NULL,    
 @DataActive = @RecordActive, @Created = @UTCDate, @Updated = @UTCDate,
 @FieldID = @FieldID, @ChangeCount = 0, @ChangeCode = NULL, @DataPointID = @DataPointID output,    
 @Guid = @guid, @LockTime = NULL, @IsVisible = @IsVisible,    
 @MissingCode = NULL, @IsTouched = 0, @IsNonConformant = 0,    
 @ReqVerification = 0, @IsVerified = 0, @ReqTranslation = 0,    
 @ReqCoding = 0, @ReqCoderCoding = 0, @WasSigned = 0, @IsSignatureCurrent = 0,    
 @SignatureLevel = NULL, @IsFrozen = @IsFrozen, @IsLocked = 0,    
 @EntryLocale = 'eng', @AnalyteRangeID = NULL, @IsDeleted = 0,    
 @AlertRangeID = NULL, @ReferenceRangeID = NULL, @RangeStatus = 0,    
 @IsHidden = @IsHidden, @LastEnteredDate = NULL, @DataPageId = @DataPageID,    
 @InstanceId = @InstanceID, @SubjectId = @SubjectID, @StudySiteId = @StudySiteID,    
 @StudyId = @StudyID, @IsUserDeactivated = NULL, @EnteredLabUnitID = NULL,    
 @LabUnitID = NULL
select @tmpStr = dbo.fnLocalString('DataPoint Inserted (WR 2104038)', @Locale)     
   insert into Audits (ObjectID, ObjectTypeID, Property, Value, Readable, AuditUserID, AuditTime, AuditSubCategoryID)  
   values (@DataPointID, 1, 'data', '', @tmpStr, @UserID, @UtcDate, 74)  
     
   if @DataFormat = 'eSigFolder'  
    update Instances set RequiresSignature = 1 where InstanceID = @InstanceID and RequiresSignature <> 1  
   else if @DataFormat = 'eSigPage'  
    update DataPages set RequiresSignature = 1 where DataPageID = @DataPageID and RequiresSignature <> 1  
   else if @DataFormat = 'eSigSubject'  
    update Subjects set RequiresSignature = 1 where SubjectID = @SubjectID and RequiresSignature <> 1  
   fetch next from Field_Cursor into @FieldID, @VariableID, @IsVisible, @IsLog, @DerivationID, @IsFrozen, @IsHidden, @DefaultValue, @DataDictID, @DataFormat  
  end  
  close Field_Cursor  
  deallocate Field_Cursor  
  fetch next from Record_Cursor into @RecordID, @FormID, @DataPageID, @InstanceID, @RecordActive, @RecordPosition  
 end  
 close Record_Cursor  
 deallocate Record_Cursor  
end try  
begin catch  
 declare @error_msg nvarchar(2000), @error_number int  
 select @error_msg = ERROR_MESSAGE(), @error_number = ERROR_NUMBER()  
 select @error_msg = cast(@error_number as nvarchar) + '(cspFillInMissingDataPoints_WR2104038): ' + @error_msg  
 raiserror (@error_msg, 16, 1)  
end catch  
Go
----------------------------------------------------------------------------

---------------------------------------------------------------
-- Select All Datapages where there are records in rec.position = 0 and rec.position > 0
-- datapoints don't exist in record position 0 but they exit in rec. position > 0
---------------------------------------------------------------
declare @error_number int, 
		@error_message nvarchar(2000),
		@SubjectID int,
		@DataPageID int

-- Create Table 
if exists (select null from sys.objects where type = 'U' 
		and name = N'Subjects_TOupdate')
drop table Subjects_TOupdate

create table Subjects_TOupdate(SubjectID int, DataPageID int, FormID int)
 
		insert into Subjects_TOupdate
		select dpg.SubjectID, dpg.DataPageID, dpg.FormID
		from dbo.Projects p
		inner join dbo.Studies st on p.ProjectID = st.ProjectID
		inner join dbo.StudySites ss on st.StudyID = ss.StudyID
		inner join dbo.Sites si on si.SiteID = ss.SiteID
		inner join dbo.Subjects s on ss.StudySiteID = s.StudySiteID
		inner join dbo.DataPages dpg on s.SubjectID = dpg.SubjectID
		inner join dbo.Forms f on f.formID = dpg.FormID
		inner join dbo.Instances i on i.InstanceID = dpg.InstanceID
		inner join dbo.Folders fld on fld.FolderID = i.FolderID
		inner join dbo.Records r on r.DataPageID = dpg.DataPageID
		inner join dbo.DataPoints dp on dp.RecordID = r.RecordID
		inner join (select r.DataPageID
					from dbo.Records r
					left join dbo.DataPoints dp on dp.RecordID = r.RecordID
					where dp.DatapointID is NULL
						and r.deleted = 0 
					group by r.DataPageID) dt on dt.DataPageID = dpg.DataPageID
			where ---dpg.datapageid = 5065612
		dbo.fnlocaldefault(projectname) = 'ZERO_AF'
		and dbo.fnlocaldefault(environmentnameid) = 'DEV'
		and f.OID = 'AE'
		and dbo.fnLocalizedInstanceName('eng',dpg.InstanceID) = 'Adverse Event (2)'
		and s.SubjectName in ('EN 1630ZA004_Copy','EN 1798ZA029_Copy')
		--and r.RecordPosition > 0
		and dp.deleted = 0
		and dpg.deleted = 0
		and r.deleted = 0
		group by dpg.SubjectID,dpg.DataPageID, dpg.FormID


----------------------------------------------------------
set @error_number = 0

--------- inserting missing datapoints

		declare cDPmissing cursor for
		select distinct SubjectID, DataPageID from Subjects_TOupdate
		open cDPmissing
		fetch next from cDPmissing into @SubjectID, @DataPageID
		while @@fetch_status = 0
		begin 
			begin transaction
				begin try
					exec cspFillInMissingDataPoints_WR2104038 @SubjectID, @DataPageID, 'eng'
					fetch next from cDPmissing into @SubjectID, @DataPageID
				end try

				begin catch
					select @error_number = error_number()
					select @error_message = error_message()
				end catch

				if @error_number = 0
					begin
						commit transaction
					end
				else
					begin
						rollback transaction
						print @error_message
					end
		end
		close cDPmissing
		deallocate cDPmissing


--*************************************************************************
---- result : 
	
select * from Subjects_TOupdate t

			
			
-- Set the Site dirty
declare		@StudyTypeID int,
			@StudySiteTypeID int,
			@SubjectTypeID int,
			@InstanceTypeID int,
			@DatapageTypeID int,
			@RecordTypeID int,
			@DatapointTypeID int

select @StudyTypeID = ObjectTypeID from objecttyper where ObjectName = 'Medidata.Core.Objects.Study'
select @StudySiteTypeID = ObjectTypeID from objecttyper where ObjectName = 'Medidata.Core.Objects.StudySite'
select @SubjectTypeID = ObjectTypeID from objecttyper where ObjectName = 'Medidata.Core.Objects.Subject'
select @InstanceTypeID = ObjectTypeID from objecttyper where ObjectName = 'Medidata.Core.Objects.Instance'
select @DatapageTypeID = ObjectTypeID from objecttyper where ObjectName = 'Medidata.Core.Objects.DataPage'
select @RecordTypeID = ObjectTypeID from objecttyper where ObjectName = 'Medidata.Core.Objects.Record'
select @DatapointTypeID = ObjectTypeID from objecttyper where ObjectName = 'Medidata.Core.Objects.DataPoint'

select st.StudyID, ss.StudySiteID, s.SubjectID, dpg.DataPageID, r.RecordID, dp.DataPointID
into #tmp
from dbo.Projects p
inner join dbo.Studies st on st.ProjectID = p.ProjectID
inner join dbo.StudySites ss on ss.StudyID = st.StudyID
inner join dbo.Sites si on si.SiteID = ss.SiteID
inner join dbo.Subjects s on s.StudySiteID = ss.StudySiteID
inner join dbo.DataPages dpg on dpg.SubjectID = s.SubjectID
inner join Subjects_TOupdate t on t.subjectid = dpg.SubjectID
inner join dbo.Records r on r.DataPageID = dpg.DataPageID
left join dbo.DataPoints dp on dp.RecordID = r.RecordID 

select instanceID
into #instances
from #tmp t
inner join instances i on i.subjectID = t.subjectID  
	

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #tmp t
inner join objectstatusallroles os on os.ObjectId = t.DataPointID and os.ObjectTypeId = @DatapointTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #tmp t
inner join objectstatusallroles os on os.ObjectId = t.RecordID and os.ObjectTypeId = @RecordTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #tmp t
inner join objectstatusallroles os on os.ObjectId = t.DataPageID and os.ObjectTypeId = @DatapageTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #instances t
inner join objectstatusallroles os on os.ObjectId = t.InstanceID and os.ObjectTypeId = @InstanceTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #tmp t
inner join objectstatusallroles os on os.ObjectId = t.SubjectID and os.ObjectTypeId = @SubjectTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #tmp t
inner join objectstatusallroles os on os.ObjectId = t.StudySiteID and os.ObjectTypeId = @StudySiteTypeID

update objectstatusallroles
set ExpirationDate = '1900-01-01 00:00:00.000'
from #tmp t
inner join objectstatusallroles os on os.ObjectId = t.StudyID and os.ObjectTypeId = @StudyTypeID



update dbo.Records
set NeedsCVRefresh = 1
from #tmp t
inner join dbo.Records r on r.RecordID = t.RecordID



--------------------------------------------------------------------------- 
-- force status recalculations
---------------------------------------------------------------------------

select distinct StudySiteID, -2 as RoleID, getUTCDate() as Dirty, studyID
into #StatusRollupQueue
from #tmp

update t 
set t.RoleID = ro.roleID
from #StatusRollupQueue t 
join userStudyRole usr		on usr.studyID = t.studyID	
join roles ro on ro.roleID = usr.roleID  and ro.active = 1	and ro.roleID > 0


INSERT INTO StatusRollupQueue (ObjectTypeID, ObjectID, RoleID, Dirty)
SELECT distinct @StudySiteTypeID, StudySiteID, RoleID, getUTCDate()
FROM #StatusRollupQueue
	


-- Clean up
drop table #tmp
drop table #instances
drop table #StatusRollupQueue

			
-- Clean up

drop table Subjects_TOupdate
drop procedure cspFillInMissingDataPoints_WR2104038

go

