from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import os

class DataIngestion:

    def parse_method(self, string_input):
        values = re.split(",",
                          re.sub('\r\n', '', re.sub(u'"', '', string_input)))
        sum = 0
        for i in range(2,12):
          sum = sum + int(values[i])
        avg = sum/10
        values.append(avg)
        sum = 0
        for i in range(2,6):
          sum = sum + int(values[i])
        if avg > sum:
          values.append('TRUE')
        else:
          values.append('FALSE')
        row = dict(
            zip(('user_id', 'city', 'a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10','summation','is_greater'),
                values))
        return row
        
        
def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        default='gs://nava-dataflow/input_dataJun-18-2020.csv',
        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='users_data.test_data')

    known_args, pipeline_args = parser.parse_known_args(argv)

    data_ingestion = DataIngestion()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (
    p | 'Read File from GCS' >> beam.io.ReadFromText(known_args.input,
                                                skip_header_lines=1)
    
    | 'String To BigQuery Row' >>
    beam.Map(lambda s: data_ingestion.parse_method(s))
    | 'Write to BigQuery' >> beam.io.Write(
        beam.io.BigQuerySink(

            known_args.output,


            schema='user_id:INTEGER,city:STRING,a1:INTEGER,a2:INTEGER,a3:INTEGER,a4:INTEGER,a5:INTEGER,a6:INTEGER,a7:INTEGER,a8:INTEGER,a9:INTEGER,a10:INTEGER,summation:FLOAT,is_greater:BOOL',

            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()