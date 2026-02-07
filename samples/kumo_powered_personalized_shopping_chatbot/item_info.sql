CREATE OR REPLACE TABLE KUMO_CHATBOT_NAISHA.PUBLIC.ITEM_INFO AS
SELECT
    article_id AS item_id,
    (
        'Item ' || article_id || ' is called ''' || prod_name || 
        '''. It is a ' || product_type_name || ' (product type number ' || product_type_no || 
        ') under the ' || product_group_name || ' product group. This item has a graphical appearance of ' || 
        graphical_appearance_name || ' (appearance number ' || graphical_appearance_no || 
        ') and belongs to the ' || colour_group_name || ' colour group (colour group code ' || 
        colour_group_code || '). It is perceived as ' || perceived_colour_value_name || 
        ' (perceived colour value ID ' || perceived_colour_value_id || 
        ') and categorized under the ' || perceived_colour_master_name || 
        ' master colour (master colour ID ' || perceived_colour_master_id || 
        '). The item is in the ' || department_name || ' department (department number ' || 
        department_no || ') of ' || index_name || ' (index code ' || 
        index_code || '). Specifically, it is in the ' || section_name || 
        ' section (section number ' || section_no || '). The garment group is ' || 
        garment_group_name || ' (garment group number ' || garment_group_no || ').'
    ) AS item_info
FROM
    KUMO_CHATBOT_NAISHA.PUBLIC.ARTICLES;