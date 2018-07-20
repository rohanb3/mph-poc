exec deplo_control_pkg.check_version ('ENG', '<DB_SCHEMA_VERSION>');
-----------------------------------------
CREATE OR REPLACE PACKAGE             "PIL_PARAL_EXEC_PKG" as 
  type t_string_array is table of varchar2(60) INDEX BY PLS_INTEGER;
  type t_huge_string_array is table of varchar2(10000) INDEX BY PLS_INTEGER;
  type t_int_array is table of PLS_INTEGER INDEX BY PLS_INTEGER;
  type t_clob_array is table of clob INDEX BY PLS_INTEGER;
  
  type t_decision is table of EG_DECISION%rowtype index by binary_integer;

  
  c_enabled_state constant varchar2 (9):= 'ENABLED'; 
  c_pending_state constant varchar2 (9):= 'PENDING';
  c_active_state constant varchar2 (9):= 'ACTIVE';
  c_broken_state constant varchar2 (9):= 'BROCKEN';
  c_complete_state constant varchar2 (9):= 'COMPLETED';
  c_populated_state constant varchar2 (9):= 'POPULATED';
  c_running_state constant varchar2 (9):= 'RUNNING';
  c_processing_state constant varchar2 (20):= 'PROCESSING';
  c_started_state constant varchar2 (9):= 'STARTED'; 
  c_failed_state constant varchar2 (9):= 'FAILED';
  c_finished_state   constant varchar2 (9):= 'FINISHED';
  c_approved_state constant varchar2 (9):= 'APPROVED'; 
  c_rejected_state constant varchar2 (9):= 'REJECTED'; 
  v_singleton pls_integer ;
  
   
  PRAGMA SERIALLY_REUSABLE;
  
  
  PROCEDURE EVENT_GENERATION_PROC (p_ERROR_STATUS IN OUT integer);
  
  PROCEDURE create_jobs_lev_1 (p_commis_period in varchar2,
                               p_trial_id in EG_TRIALRUNDETAILS.PIL_TRIALRUN_ID%type );
  PROCEDURE create_jobs_lev_2(
      p_event_type    IN VARCHAR2,
      p_commis_period IN VARCHAR2,
      p_trial_id      IN EG_TRIALRUNDETAILS.PIL_TRIALRUN_ID%type);
      
    PROCEDURE processing_level_3(
      p_event_type    IN VARCHAR2,
      p_commis_period IN VARCHAR2,
      p_event_id      IN VARCHAR2 ,
      p_trial_id      IN EG_TRIALRUNDETAILS.PIL_TRIALRUN_ID%type);    
   procedure set_singleton;
   function get_singleton return pls_integer;
   
   function EG_LVL1_TRINIT (p_trialrun_setup in PIL_TRIALRUN_PKG.t_trialrun_setup,
                             p_trialrun_id in EG_TRIALRUNDETAILS.PIL_TRIALRUN_ID%type) return pls_integer;
                             
                    
end pil_paral_exec_pkg;
/


CREATE OR REPLACE PACKAGE BODY              "PIL_PARAL_EXEC_PKG"
AS
  PRAGMA SERIALLY_REUSABLE;



function get_level_serial_jobs (p_event_type varchar2, p_job_level in pls_integer)
return t_decision
is
  vt_decision t_decision;
begin
  select * bulk collect into vt_decision from eg_decision
  where proc_level = p_job_level and STATUS = c_active_state and eventtype = p_event_type
  order by CALL_SEQ asc;
  
  return vt_decision;
end get_level_serial_jobs;


function get_unique_event_types
return t_string_array
is
  vt_event_types t_string_array;
begin
  select distinct eventtype bulk collect into vt_event_types from eg_decision
  where  STATUS = c_active_state;
  
  return vt_event_types;
end get_unique_event_types;

function get_unique_event_ids (p_event_type in varchar2)
return t_int_array
is
  vt_event_ids t_int_array;
begin
  select distinct eventcode bulk collect into vt_event_ids from eg_decision
  where  STATUS = c_active_state and eventtype=p_event_type and eventcode is not NULL and eventcode !='NULL';
  
  return vt_event_ids;
end get_unique_event_ids;


function get_businessunit_from_trialrun (p_trialrun_id in   EG_TRIALRUNDETAILS.PIL_TRIALRUN_ID%type)
        return EG_DECISION.BUSINESSUNIT%type
    is 
       v_JSONTRIALRUNPARAM     EG_TRIALRUNDETAILS.JSONTRIALRUNPARAM%type;
       v_businessunit EG_DECISION.BUSINESSUNIT%type;
       v_trialrun_setup PIL_TRIALRUN_PKG.T_TRIALRUN_SETUP;
       
    begin
        select JSONTRIALRUNPARAM into v_JSONTRIALRUNPARAM from EG_TRIALRUNDETAILS where PIL_TRIALRUN_ID = p_trialrun_id;
       v_trialrun_setup := PIL_TRIALRUN_PKG.PARSE_TRIALRUN_SETUP(v_JSONTRIALRUNPARAM);
       v_businessunit := v_trialrun_setup.business_unit;
       return   v_businessunit;
    end get_businessunit_from_trialrun;


procedure start_serial_jobs (p_job_level in pls_integer,
                             p_trialrun_id in EG_TRIALRUNDETAILS.PIL_TRIALRUN_ID%type,
                             p_event_type in varchar2 default null,
                             p_event_id in VARCHAR2 default null)
is 
  vt_decision t_decision;
  v_sql clob;
  v_businessunit EG_DECISION.BUSINESSUNIT%type;
begin
  vt_decision := get_level_serial_jobs (p_event_type, p_job_level) ;
  v_businessunit := get_businessunit_from_trialrun (p_trialrun_id);
  IF vt_decision.COUNT > 0 THEN
	for v_i in vt_decision.first .. vt_decision.last
	loop
    if ((vt_decision(v_i).BUSINESSUNIT is null or vt_decision(v_i).BUSINESSUNIT = v_businessunit )
		and vt_decision(v_i).eventtype = p_event_type 
		and  vt_decision(v_i).eventcode = p_event_id  
		and  p_job_level = 3)
       or  ( p_event_type is null and p_event_id is null and  p_job_level = 1  )
       or  ( vt_decision(v_i).eventtype = p_event_type and p_event_id is null and  p_job_level = 2  )then
			--v_sql := 'begin '|| vt_decision(v_i).STOR_PROC_NAME||' (' ||p_trialrun_id||'); end;';       
			if (p_event_id is NULL OR trim(p_event_id) = '') AND  p_job_level != 3 then
			  v_sql := 'begin '|| vt_decision(v_i).STOR_PROC_NAME||' (' ||p_trialrun_id||'); end;';
			else
			  v_sql := 'begin '|| vt_decision(v_i).STOR_PROC_NAME||' (' ||p_trialrun_id||' , '''|| p_event_id||'''); end;';
			end if;
          pil_dynamic_sql.exec_dynamic_sql (v_sql);
       elsif   p_job_level != 3  then
        pil_logger_pkg.cmn_log_error (pil_log_messages_pkg.c_msg_wrong_proc_call_nbr, pil_log_messages_pkg.c_msg_wrong_proc_call_txt, pil_global_pkg.t_string_list('start_serial_jobs'));
    end if;
  end loop;
  END IF;
  end start_serial_jobs; 
  
  
function EG_LVL1_TRINIT (p_trialrun_setup in PIL_TRIALRUN_PKG.t_trialrun_setup,
  p_trialrun_id in EG_TRIALRUNDETAILS.PIL_TRIALRUN_ID%type)
  return pls_integer
  IS
  v_sql clob;
  v_decision_id pls_integer;
  v_rowcount pls_integer;
  Begin
      v_sql :=pil_trialrun_pkg.dyn_insert_as_select (p_trialrun_setup,p_trialrun_id);
      v_rowcount := pil_dynamic_sql.exec_dynamic_sql (v_sql);
      if v_rowcount > 0 then
          PIL_TRIALRUN_PKG.update_trace_table (p_trialrun_id);
      end if;
      pil_trialrun_pkg.update_status (p_trialrun_id,c_running_state);
      pil_logger_pkg.cmn_log_info (p_code => pil_log_messages_pkg.c_msg_dsql_trace_trial_run_nbr,
          p_message => pil_log_messages_pkg.c_msg_dsql_trace_trial_run_txt,
          p_param_list => pil_global_pkg.t_string_list(v_rowcount,p_trialrun_id)
          );
      
  return v_rowcount;
      
  end EG_LVL1_TRINIT;
  
  PROCEDURE EVENT_GENERATION_PROC (p_ERROR_STATUS IN OUT integer)
   IS
    v_instance_var pls_integer;
    v_trialrun_rec EG_TRIALRUNDETAILS%rowtype;
    v_trialrun_setup PIL_TRIALRUN_PKG.t_trialrun_setup;
    v_sql clob;
    v_rowcount pls_integer;
	v_errorrecord_return  number;
	v_cnt number;
	v_err_msg varchar2(100);
   begin
    DBMS_APPLICATION_INFO.SET_MODULE ('PIL_PARAL_EXEC_PKG','EVENT_GENERATION_PROC');
	p_ERROR_STATUS := 0;
	
	SELECT count(1) 
	  INTO v_cnt
	  FROM eg_trialrundetails
	 WHERE status = 'RUNNING';
    IF 	v_cnt = 0 THEN 
    v_instance_var := PIL_PARAL_EXEC_PKG.get_singleton;
   if  v_instance_var = 5 then
      PIL_PARAL_EXEC_PKG.set_singleton;
      v_trialrun_rec := pil_trialrun_pkg.get_workload;
      if  (v_trialrun_rec.pil_trialrun_id is null) then
          pil_logger_pkg.cmn_log_info (p_code => pil_log_messages_pkg.c_msg_dsql_no_workload_nbr,
          p_message => pil_log_messages_pkg.c_msg_dsql_no_workload_text,
          p_param_list => pil_global_pkg.t_string_list(sysdate)
          );
          RAISE_APPLICATION_ERROR(pil_log_messages_pkg.c_msg_dsql_no_workload_nbr, pil_log_messages_pkg.c_msg_dsql_no_workload_text);
      end if;      
      v_trialrun_setup := PIL_TRIALRUN_PKG.parse_trialrun_setup(v_trialrun_rec.JSONTRIALRUNPARAM);
      v_rowcount := EG_LVL1_TRINIT (v_trialrun_setup, v_trialrun_rec.PIL_TRIALRUN_ID); 
	  
	  IF v_rowcount  > 100000 THEN
         v_errorrecord_return := PIL_COMMON_PKG.CMN_INSERT_ERRORRECORDS(v_trialrun_rec.PIL_TRIALRUN_ID,'TBL1035','PIL33001','EG',NULL,NULL);
		 p_ERROR_STATUS := 1;
      ELSIF v_rowcount = 0 THEN 
         v_errorrecord_return := PIL_COMMON_PKG.CMN_INSERT_ERRORRECORDS(v_trialrun_rec.PIL_TRIALRUN_ID,'TBL1010','PIL33002','EG',NULL,NULL);
		 p_ERROR_STATUS := 1;
      END IF;
	  
      if v_rowcount >0 then
         create_jobs_lev_1 ( v_trialrun_setup.commission_period,v_trialrun_rec.PIL_TRIALRUN_ID);
	  else
		PIL_TRIALRUN_PKG.update_status (v_trialrun_rec.PIL_TRIALRUN_ID,c_complete_state);
      end if;
   else
     RAISE_APPLICATION_ERROR(-20000, 'One instance is running already', TRUE );
	 p_ERROR_STATUS := 1;
   end if; 
   ELSE
   SELECT 'Trial Run id : '|| pil_trialrun_id  || ' was Running'
	  INTO v_err_msg
	  FROM eg_trialrundetails
	 WHERE status = 'RUNNING';
   pil_logger_pkg.CMN_LOG_ERROR(9999,v_err_msg ,null,null);
   p_ERROR_STATUS := 1;
 END IF;
   
   end EVENT_GENERATION_PROC;
  
  PROCEDURE create_jobs_lev_1(
      p_commis_period IN VARCHAR2,
      p_trial_id      IN EG_TRIALRUNDETAILS.PIL_TRIALRUN_ID%type )
  IS
    JobNo user_jobs.job%TYPE;
    vt_eventtype t_string_array;
    v_what    varchar2(2000);
    v_description varchar2(2000) ;
    v_next_day date := sysdate+1/52000 ;
    v_interval varchar2(200) := NULL;
    
  BEGIN
      DBMS_APPLICATION_INFO.SET_MODULE ('PIL_PARAL_EXEC_PKG','CREATE_JOBS_LEVEL_1');
      vt_eventtype :=  get_unique_event_types ;
      start_serial_jobs (1,p_trial_id);
    IF vt_eventtype.count  > 0 THEN
      FOR v_i IN vt_eventtype.first..vt_eventtype.last
      LOOP
		BEGIN
			v_description := 'CREATE_JOBS_LEVEL_1' ;
			v_what := 'begin PIL_PARAL_EXEC_PKG.CREATE_JOBS_LEV_2('''||vt_eventtype(v_i)||''','''||p_commis_period||''','||p_trial_id||'); end;';
			pil_eg_job_control_pkg.submit_job (p_job => JobNo,
			  p_what             => v_what,
			  p_description      => v_description,
			  p_next_date        => v_next_day,
			  p_interval         => v_interval,
			  p_TRIALRUN_ID      => p_trial_id,
			  p_EVENTTYPE        => vt_eventtype(v_i) ,
			  p_event_id         => null,
			  p_COMMISSIONPERIOD => p_commis_period,
			  p_job_level        => 1,
			  p_parent_job       => null
			);        
		EXCEPTION
		WHEN OTHERS THEN
			pil_logger_pkg.CMN_LOG_ERROR(SQLCODE,SUBSTR(SQLERRM,0, 100) || ';v_what: '||v_what,null,null); 
		END;
      END LOOP;
    END IF;
  END create_jobs_lev_1;
  
  PROCEDURE create_jobs_lev_2(
      p_event_type    IN VARCHAR2,
      p_commis_period IN VARCHAR2,
      p_trial_id      IN EG_TRIALRUNDETAILS.PIL_TRIALRUN_ID%type)
  IS
    JobNo user_jobs.job%TYPE;
    vt_eventid t_int_array;
    v_what varchar2(2000) ;
    v_description varchar2(2000) ;
    v_next_day date := sysdate+1/52000 ;
    v_interval varchar2(200) := NULL;
  BEGIN
    DBMS_APPLICATION_INFO.SET_MODULE ('PIL_PARAL_EXEC_PKG','create_jobs_lev_2');
    vt_eventid := get_unique_event_ids (p_event_type);
    start_serial_jobs (2,p_trial_id,p_event_type);
    IF vt_eventid.count    > 0 THEN
      FOR v_i IN vt_eventid.first..vt_eventid.last
      LOOP
		BEGIN
			v_description := 'create_jobs_lev_2' ;
			v_what := 'begin PIL_PARAL_EXEC_PKG.processing_level_3('''||p_event_type||''','''||p_commis_period||''','''||vt_eventid(v_i)||''','||p_trial_id||'); end;';
			pil_eg_job_control_pkg.submit_job (p_job => JobNo,
			  p_what             => v_what,
			  p_description      => v_description,
			  p_next_date        => v_next_day,
			  p_interval         => v_interval,
			  p_TRIALRUN_ID      => p_trial_id,
			  p_EVENTTYPE        => p_event_type ,
			  p_event_id         => vt_eventid(v_i),
			  p_COMMISSIONPERIOD => p_commis_period,
			  p_job_level        => 2,
			  p_parent_job       => null
			);
		EXCEPTION
		WHEN OTHERS THEN
			pil_logger_pkg.CMN_LOG_ERROR(SQLCODE,SUBSTR(SQLERRM,0, 100) || ';v_what: '||v_what,null,null); 	
        END;			
      END LOOP;
    END IF;
  END create_jobs_lev_2;
 
  
  PROCEDURE processing_level_3(
      p_event_type    IN VARCHAR2,
      p_commis_period IN VARCHAR2,
      p_event_id      IN VARCHAR2 ,
      p_trial_id      IN EG_TRIALRUNDETAILS.PIL_TRIALRUN_ID%type)
  IS
    --vt_eventrecords_id t_string_array;
    vt_processing_steps t_string_array;
    v_retval pls_integer;
  BEGIN
      DBMS_APPLICATION_INFO.SET_MODULE ('PIL_PARAL_EXEC_PKG','processing_level_3');
      start_serial_jobs (3,p_trial_id,p_event_type,p_event_id);
  END processing_level_3;
  
  procedure set_singleton
  is 
  begin
    v_singleton := v_singleton*2;
  end set_singleton;
  function get_singleton return pls_integer 
  is 
  begin
    return v_singleton;
  end get_singleton;
    
  
begin
 v_singleton  := 5;  
END pil_paral_exec_pkg;
/

