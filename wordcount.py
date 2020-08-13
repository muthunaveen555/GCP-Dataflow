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


class Split(beam.DoFn):

    def process(self, element):
        """
        Splits each row on commas and returns a dictionary representing the
        row
        """
        att_id, name, user_num, department, date = element.split(",")

        return [{
            'name': name,
            'att_id': att_id,
            'department': department
        }]

class CollectUsers(beam.DoFn):

    def process(self, element):
        result = [
            (element['name'], element['att_id'])
        ]
        return result


def has_department(ele, depart1, depart2):
  return (ele['department'] == depart1 or ele['department'] == depart2)


def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='gs://nava-dataflow/input_dept_data.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      required=True,
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  
  with beam.Pipeline(options=pipeline_options) as p:
    rows = (
                p |
                'Read' >> ReadFromText(known_args.input) |
                beam.ParDo(Split())
            )
    filtered = (
                  rows |
                  'Filtering Department' >> beam.Filter(has_department, 'Accounts','HR')

    )        
    users = (
                filtered |
                beam.ParDo(CollectUsers()) |
                "Grouping users" >> beam.GroupByKey() |
                "Counting users" >> beam.CombineValues(
                    beam.combiners.CountCombineFn()
                )
            )
    output = (
                users |
                'Write' >> WriteToText(known_args.output)
            )
    


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()