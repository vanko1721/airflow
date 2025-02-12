from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator # type: ignore
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

# 기본 매개변수 설정
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "snowflake_conn_id": "snowflake_conn",  # Snowflake 연결 ID
}

# 매개변수 정의
P_FR_DT = "2024-11-03"
P_TO_DT = "2024-11-04"

# DAG 객체 생성
with DAG(
    dag_id="Snowflake_Batch_Dags_Four",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # 수동 또는 최초 실행만
    catchup=False,
    description="Snowflake을 사용한 배치 작업 DAG",
    tags=["Snowflake", "Batch", "ETL"],
) as dag:

    ############################################
    # DEPTH 1
    ############################################
    with TaskGroup(group_id="depth_1") as depth_1:
        RAW_USP_REACTING2_EVNT = SnowflakeOperator(
            task_id="RAW_USP_REACTING2_EVNT",
            sql=f"CALL RAW.USP_REACTING2_EVNT('{P_FR_DT}', '{P_TO_DT}');",
            autocommit=True,
        )

    ############################################
    # DEPTH 2
    ############################################
    with TaskGroup(group_id="depth_2") as depth_2:
        ########################################
        # TG_ORD_ITEM: DW.USP_ORD_ITEM (병렬 수행 및 순차 실행)
        ########################################
        with TaskGroup(group_id="TG_ORD_ITEM") as tg_ord_item:
            # DW.USP_POC_DW_ORD_ITEM_01
            USP_POC_DW_ORD_ITEM_01 = SnowflakeOperator(
                task_id="USP_POC_DW_ORD_ITEM_01",
                sql=f"CALL DW.USP_ORD_ITEM('{P_FR_DT}', '{P_TO_DT}', 1);",
                autocommit=True,
            )

            # DW.USP_POC_DW_ORD_ITEM_02 ~ DW.USP_POC_DW_ORD_ITEM_08 (병렬 수행)
            USP_POC_DW_ORD_ITEMS_02_08 = [
                SnowflakeOperator(
                    task_id=f"USP_POC_DW_ORD_ITEM_{i:02}",
                    sql=f"CALL DW.USP_ORD_ITEM('{P_FR_DT}', '{P_TO_DT}', {i});",
                    autocommit=True,
                )
                for i in [2, 3, 4, 5, 7, 8]
            ]

            # DW.USP_POC_DW_ORD_ITEM_06 (선행: USP_POC_DW_ORD_ITEM_01)
            USP_POC_DW_ORD_ITEM_06 = SnowflakeOperator(
                task_id="USP_POC_DW_ORD_ITEM_06",
                sql=f"CALL DW.USP_ORD_ITEM('{P_FR_DT}', '{P_TO_DT}', 6);",
                autocommit=True,
            )

            # DW.USP_POC_DW_ORD_ITEM_11 (선행: USP_POC_DW_ORD_ITEM_06)
            USP_POC_DW_ORD_ITEM_11 = SnowflakeOperator(
                task_id="USP_POC_DW_ORD_ITEM_11",
                sql=f"CALL DW.USP_ORD_ITEM('{P_FR_DT}', '{P_TO_DT}', 11);",
                autocommit=True,
            )

            # DW.USP_POC_DW_ORD_ITEM_99 (병렬 작업 모두 완료 후)
            USP_POC_DW_ORD_ITEM_99 = SnowflakeOperator(
                task_id="USP_POC_DW_ORD_ITEM_99",
                sql=f"CALL DW.USP_ORD_ITEM('{P_FR_DT}', '{P_TO_DT}', 99);",
                autocommit=True,
            )

            # 의존성 설정
            USP_POC_DW_ORD_ITEM_01 >> USP_POC_DW_ORD_ITEM_06 >> USP_POC_DW_ORD_ITEM_11
            USP_POC_DW_ORD_ITEM_01 >> USP_POC_DW_ORD_ITEMS_02_08 >> USP_POC_DW_ORD_ITEM_99
            USP_POC_DW_ORD_ITEM_11 >> USP_POC_DW_ORD_ITEM_99

        # DW_USP_ITEM: DW.USP_ITEM (Depth 2, 선행: TG_ORD_ITEM)
        DW_USP_ITEM = SnowflakeOperator(
            task_id="DW_USP_ITEM",
            sql=f"CALL DW.USP_ITEM('{P_FR_DT}', '{P_TO_DT}');",
            autocommit=True,
        )

        # DW_USP_SHPP_ITEM: DW.USP_SHPP_ITEM (Depth 2, 선행: DW_USP_ITEM)
        DW_USP_SHPP_ITEM = SnowflakeOperator(
            task_id="DW_USP_SHPP_ITEM",
            sql=f"CALL DW.USP_SHPP_ITEM('{P_FR_DT}', '{P_TO_DT}');",
            autocommit=True,
        )

        # DMAND_PORD_PREDT_DC_ITEM: DMAND_PREDT.USP_PORD_PREDT_DC_ITEM (Depth 2, 독립)
        DMAND_PORD_PREDT_DC_ITEM = SnowflakeOperator(
            task_id="DMAND_PORD_PREDT_DC_ITEM",
            sql=f"CALL DMAND_PREDT.USP_PORD_PREDT_DC_ITEM('{P_FR_DT}', '{P_TO_DT}');",
            autocommit=True,
        )

        # 의존성 설정: TG_ORD_ITEM >> DW_USP_ITEM >> DW_USP_SHPP_ITEM
        tg_ord_item >> DW_USP_ITEM >> DW_USP_SHPP_ITEM

    ############################################
    # DEPTH 3
    ############################################
    with TaskGroup(group_id="depth_3") as depth_3:
        USP_REACT_EATERY_CLICK_ACTRLT_ACCUM = SnowflakeOperator(
            task_id="BI_UNIT_USP_REACT_EATERY_CLICK_ACTRLT_ACCUM",
            sql=f"CALL BI_UNIT.USP_REACT_EATERY_CLICK_ACTRLT_ACCUM('{P_FR_DT}', '{P_TO_DT}');",
            autocommit=True,
        )

        USP_CMT_ORDER_DTL = SnowflakeOperator(
            task_id="CAMP_USP_CMT_ORDER_DTL",
            sql=f"CALL CAMP.USP_CMT_ORDER_DTL('{P_FR_DT}', '{P_TO_DT}');",
            autocommit=True,
        )

        USP_PRC_OPTI_PROM_CST_ACTRLT = SnowflakeOperator(
            task_id="PRC_USP_PRC_OPTI_PROM_CST_ACTRLT",
            sql=f"CALL PRC.USP_PRC_OPTI_PROM_CST_ACTRLT('{P_FR_DT}', '{P_TO_DT}');",
            autocommit=True,
        )

        USP_EADD_ITEM_CNT_ACCUM_1 = SnowflakeOperator(
            task_id="BI_UNIT_USP_EADD_ITEM_CNT_ACCUM_1",
            sql=f"CALL BI_UNIT.USP_EADD_ITEM_CNT_ACCUM_1('{P_FR_DT}', '{P_TO_DT}');",
            autocommit=True,
        )

        with TaskGroup(group_id="TG_ALEX_CMS") as tg_alex_cms:
            ALEX_CMS_SNOWFLAKE_TASKS = [
                SnowflakeOperator(
                    task_id=f"BI_UNIT_USP_ALEX_CMS_{task_id}",
                    sql=f"CALL BI_UNIT.USP_ALEX_CMS_{task_id}('{P_FR_DT}', '{P_TO_DT}');",
                    autocommit=True,
                )
                for task_id in [
                    "002", "003", "004", "005", "006", "007_01", "008", "009",
                    "012", "014", "015", "015_GEOMETRY_DIM", "015_SELECT",
                    "018", "020", "020_01", "022", "023", "023_01", "024", "029"
                ]
            ]

        BI_UNIT_USP_ALEX_MBR_032 = SnowflakeOperator(
            task_id="BI_UNIT_USP_ALEX_MBR_032",
            sql=f"CALL BI_UNIT.USP_ALEX_MBR_032('{P_FR_DT}', '{P_TO_DT}');",
            autocommit=True,
         ) 

    ############################################
    # 전체 DAG 의존성 설정: DEPTH 1 -> DEPTH 2 -> DEPTH 3
    ############################################
    depth_1 >> depth_2 >> depth_3