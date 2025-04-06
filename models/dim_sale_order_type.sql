{{ config(materialized='incremental') }}

    SELECT 1 AS ID, 'Standard' AS TYPEDESCRIPTION, 'Order Type 1' AS TYPENAME, 'Subtype1' AS SUBTYPEDESCRIPTION, 'AS400_01' AS AS400NAME, 'SubTypeA' AS SUBTYPENAME, 'SuperTypeX' AS SUBSUPERTYPENAME, 1001 AS LEGACYTYPEID, '2025-04-01 10:00:00+00:00' AS CREATETIME UNION
    SELECT 2, 'Express', 'Order Type 2', 'Subtype2', 'AS400_02', 'SubTypeB', 'SuperTypeY', 1002, '2025-04-02 11:30:00+00:00' UNION
    SELECT 3, 'Standard', 'Order Type 3', 'Subtype3', 'AS400_03', 'SubTypeC', 'SuperTypeZ', 1003, '2025-04-03 12:45:00+00:00' UNION
    SELECT 4, 'Urgent', 'Order Type 4', 'Subtype4', 'AS400_04', 'SubTypeD', 'SuperTypeA', 1004, '2025-04-04 14:15:00+00:00' UNION
    SELECT 5, 'Standard', 'Order Type 5', 'Subtype5', 'AS400_05', 'SubTypeE', 'SuperTypeB', 1005, '2025-04-05 15:00:00+00:00' UNION
    SELECT 6, 'Express', 'Order Type 6', 'Subtype6', 'AS400_06', 'SubTypeF', 'SuperTypeC', 1006, '2025-04-06 16:30:00+00:00' UNION
    SELECT 7, 'Urgent', 'Order Type 7', 'Subtype7', 'AS400_07', 'SubTypeG', 'SuperTypeD', 1007, '2025-04-07 17:45:00+00:00' UNION
    SELECT 8, 'Standard', 'Order Type 8', 'Subtype8', 'AS400_08', 'SubTypeH', 'SuperTypeE', 1008, '2025-04-08 18:00:00+00:00' UNION
    SELECT 9, 'Express', 'Order Type 9', 'Subtype9', 'AS400_09', 'SubTypeI', 'SuperTypeF', 1009, '2025-04-09 19:15:00+00:00' UNION
    SELECT 10, 'Standard', 'Order Type 10', 'Subtype10', 'AS400_10', 'SubTypeJ', 'SuperTypeG', 1010, '2025-04-10 20:30:00+00:00'