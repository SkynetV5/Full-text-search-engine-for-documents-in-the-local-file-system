from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import apache_beam as beam
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from app.indekser import TF, TFIDF_I_Save


def test_beam_clean_text_transform():

    input_data = [{
        "path": "docs/test.txt",
        "text": "Python jest szybki! Python!",
    }]

    expected_output = [{
        "path": "docs/test.txt",
        "tf": {"python": 2 / 3, "szybki": 1 / 3},
    }]

    with TestPipeline() as p:
        p_collection = p | "CreateInput" >> beam.Create(input_data)
        tf_output = p_collection | "ComputeTF" >> beam.ParDo(TF())

        assert_that(tf_output, equal_to(expected_output))

def test_tf_dofn_empty_text():

    input_data = [{
        "path": "docs/empty.txt",
        "text": "jest i w na",
    }]

  
    expected_output = [{
        "path": "docs/empty.txt",
        "tf": {},
    }]

    with TestPipeline() as p:
        p_collection = p | "CreateInput" >> beam.Create(input_data)
        tf_output = p_collection | "ComputeTF" >> beam.ParDo(TF())

        assert_that(tf_output, equal_to(expected_output))
    
def test_tf_idf_i_save_dofn(tmp_path):
    input_data = [{
        "path": "docs/test.txt",
        "tf": {"python": 0.5, "szybki": 0.5}
    }]

    
    total_docs_data = [2]
    df_counts_data = [("python", 1), ("szybki", 2)]

    with TestPipeline() as p:
            main_input = p | "CreateMain" >> beam.Create(input_data)
            total_docs = p | "CreateTotal" >> beam.Create(total_docs_data)
            df_counts = p | "CreateDF" >> beam.Create(df_counts_data)

            output = main_input | "SaveTFIDF" >> beam.ParDo(TFIDF_I_Save(), n_docs = beam.pvalue.AsSingleton(total_docs), df_dict = beam.pvalue.AsDict(df_counts))

            assert_that(output,equal_to(["docs/test.txt"]))

    
            
        
