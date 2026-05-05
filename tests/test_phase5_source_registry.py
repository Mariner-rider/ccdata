from services.lite_pipeline.main import merge_pages

def test_schema_mapping_profiles():
    ex={"name":"X","location":"Y","official_website":"u","courses":["B.Tech"],"fees":["$1"],"admission_link":["a"],"placement":["p"],"faculty":["f"],"hostel":["h"],"field_details":{k:{"confidence":0.9,"extraction_method":"x"} for k in ["location","courses","fees","admission_link","placement","faculty","hostel"]}}
    rec=merge_pages('college','X','file://x',[{"url":"file://x","extract":ex,"page_type":"homepage"}], 'official')
    assert rec['confidence_score']>0.9
