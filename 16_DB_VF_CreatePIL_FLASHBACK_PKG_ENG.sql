-----------------------------------------
-- NAME: <16_DB_VF_CreatePIL_FLASHBACK_PKG.sql>
-- DESCRIPTION:
-----------------------------------------
-- MODIFICATION HISTORY
-- 1.0
-----------------------------------------
-- IMPORTANT REMARK
-----------------------------------------
-- HEADER
-----------------------------------------
exec deplo_control_pkg.check_version ('ENG', '<DB_SCHEMA_VERSION>');
-----------------------------------------
DEFINE myjob = '16_DB_VF_CreatePIL_FLASHBACK_PKG';
WHENEVER SQLERROR EXIT 1
WHENEVER OSERROR EXIT 2
col myuser noprint new_value myuser
col mydate noprint new_value mydate
col mydb noprint new_value mydb
select UPPER(USER) myuser from dual;
select to_char(sysdate,'YYYYMMDDYHH24MISS')mydate from dual;
select sys_context('USERENV','DB_NAME')mydb from dual;
SET FEEDBACK ON TIME ON TIMING ON ECHO ON HEADING ON VERIFY ON
spool Log/&myjob._&mydb._&myuser._&mydate..log
CREATE OR REPLACE PACKAGE pil_flashback_pkg
AS
  -------------------------------------------------------------------
  -- Support Package to queries using FLASHBACK feature --                                        --
  -------------------------------------------------------------------

  -- Interface Methods:                        --

  -- Save Current SCN as Extract Reference SCN --
  PROCEDURE set_flashback_scn;

  -- Save Given SCN as Extract Reference SCN   --
  PROCEDURE set_flashback_scn(p_scn IN NUMBER);

  -- Get Extract Reference SCN                 --
  FUNCTION get_flashback_scn  RETURN NUMBER;
  
  PRAGMA RESTRICT_REFERENCES (get_flashback_scn, WNPS, WNDS, RNPS);

END pil_flashback_pkg;
/


CREATE OR REPLACE PACKAGE BODY pil_flashback_pkg
AS
  -------------------------------------------------------------------
  -- Support Package to queries using FLASHBACK feature  --                                       --
  -------------------------------------------------------------------

  -- Interface Methods:                        --

  -- Save Current SCN as Extract Reference SCN --
  PROCEDURE set_flashback_scn IS
  BEGIN
    Set_flashback_scn (dbms_flashback.get_system_change_number);
  END set_flashback_scn;


  -- Save Given SCN as Extract Reference SCN   --
  PROCEDURE Set_flashback_scn(p_scn IN NUMBER)
  IS
    PRAGMA AUTONOMOUS_TRANSACTION;
  BEGIN
    -- Try to update xref_flashback_ts --
    UPDATE cmn_flashback_scn SET scn = p_scn;
    IF SQL%rowcount <> 1 THEN
      -- Re-populate xref_flashback_ts except the table contains exactly 1 row --
      DELETE cmn_flashback_scn;
      INSERT INTO cmn_flashback_scn (scn) VALUES (p_scn);
    END IF;
    COMMIT;
  END set_flashback_scn;


  -- Get Extract Reference SCN                 --
  FUNCTION Get_flashback_scn
  RETURN NUMBER
  IS
    v_scn NUMBER;
  BEGIN
    SELECT scn
    INTO   v_scn
    FROM   cmn_flashback_scn;

    RETURN v_scn;
  EXCEPTION
  WHEN no_data_found THEN
    raise_application_error(-20000, 'Unable to get flashback SCN: table CMN_FLASHBACK_SCN is empty.');
  WHEN too_many_rows THEN
    raise_application_error(-20000, 'Unable to get flashback SCN: more than one row in the table CMN_FLASHBACK_SCN.');
  END Get_flashback_scn;


END pil_flashback_pkg;
/
show errors;
PROMPT Finished creating the pil_flashback_pkg package ...

-----------------------------------------

