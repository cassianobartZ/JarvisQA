import csv
import os
from typing import List
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_string_dtype
from collections import Counter
from itertools import chain


class T2T:

    @staticmethod
    def read_csv(path: str) -> (List, List):
        if not os.path.exists(path):
            raise ValueError(f"Path <{path}> doesnt exists")
        with open(path, 'r', encoding="utf8") as in_file:
            sep = '\t' if path[-4:] == '.tsv' else ','
            reader = csv.reader(in_file, delimiter=sep)
            result = [row for row in reader]
            return result[0], result[1:]

    @staticmethod
    def clean_predicate(predicate: str) -> str:
        if predicate[:3].lower() == 'has':
            return predicate[3:]
        return predicate

    @staticmethod
    def append_value(value: str) -> str:
        if len(value.split(',')) > 1:
            return f'are "{value}"'
        else:
            return f'is "{value}"'

    def append_aggregation_info(self, df: pd.DataFrame) -> str:
        info = []
        for column in df:
            if is_numeric_dtype(df[column]):
                column_df = df[column].fillna(0)
                max_value = column_df.max()
                min_value = column_df.min()
                avg_value = column_df.mean()
                sum = column_df.sum()
                info.append(f'The maximum value of {column} is {max_value},'
                            f' the minimum value of {column} is {min_value},'
                            f' and the average value of {column} is {avg_value:.2f},'
                            f'the total of {column} is {sum}')
                # max_title = df.iloc[column_df.argmax()]['Title']
                # min_title = df.iloc[column_df.argmin()]['Title']
                # info.append(f'The paper with the maximum {column} is "{max_title}"'
                #             f' and the paper with the minimum {column} is {min_title}')
                self.append_text_for_maximum_and_minimum_numeric_column(info, column, df)
                self.append_text_for_numeric_column(info, column, df)
            if is_string_dtype(df[column]):
                column_df = df[column].fillna('')
                counts = Counter([str(value).strip() for value in chain(*[str(value).split(',') for value in column_df.values]) if len(value) > 0])
                max_occurrence = max(counts.values())
                if max_occurrence == 1:
                    # ignore one value occurrences
                    continue
                if 'Unnamed:' in column:
                    continue
                min_occurrence = min(counts.values())
                most_common = [k for k, v in counts.items() if v == max_occurrence]
                least_common = [k for k, v in counts.items() if v == min_occurrence]

                for columnValue in most_common:
                    info.append(f'The most common {column} {self.append_value(columnValue)}')

                for columnValue in least_common:
                    info.append(f'The least common {column} {self.append_value(columnValue)}')

                # # identified error: least common or most common can grab a lot of entries and by itself explode 500 tokens
                # info.append(f'The most common {column} {self.append_value(most_common[0])},'
                #             f' and the least common {self.append_value(least_common[0])}')
        return '\n'.join(info)

    
    def append_text_for_maximum_and_minimum_numeric_column(self, info: str, numericColumnName: str, df: pd.DataFrame) -> str:
        columns = list(df)
        column_df = df[numericColumnName].fillna(0)
        rowForHighestNumericValue = df.iloc[column_df.argmax()]
        rowForLowestNumericValue = df.iloc[column_df.argmin()]

        # info.append(f'The paper with the maximum {column} is "{max_title}"'
        #             f' and the paper with the minimum {column} is {min_title}')

        #  the {iterationColum/title} with the highest {numericColumn/numberOfSpecies} is {iterationColumnValue/the invertebrale...}
        #  {iterationColumn} {numericColumnName} {iterationColumnValue}

        for indexIterationColumn,iterationColumn in enumerate(columns):
            if is_numeric_dtype(df[iterationColumn]):
                continue
            lowestRowIterationColumnValue = rowForLowestNumericValue[iterationColumn]
            highestRowIterationColumnValue = rowForHighestNumericValue[iterationColumn]
            info.append(f'The {iterationColumn} with the highest {numericColumnName} is {highestRowIterationColumnValue}')
            info.append(f'The {iterationColumn} with the lowest {numericColumnName} is {lowestRowIterationColumnValue}')
            
        return info
        
    def append_text_for_numeric_column(self, info: str, numericColumnName: str, df: pd.DataFrame) -> str:
        columns = list(df)

        for indexColumnValue, columnValue in enumerate(df[numericColumnName]):
            for indexIterationColumn,iterationColumn in enumerate(columns):
                if is_numeric_dtype(df[iterationColumn]):
                    continue
                iterationColumnValue = df.iloc[indexColumnValue][iterationColumn]
                info.append(f'The {numericColumnName} of {iterationColumnValue} is {columnValue}')
                # the {numericColumnName|precision} of {iterationColumnValue|qald-6} is {columnValue|0.25}
                # the {iterationColumnValue|qald-6} has {numericColumnName|precision} of {columnValue|0.25}
                #  test scores with both

        return info


    def row_2_text(self, row: List, header: List, start_index: int = 0, empty=False) -> str:
        text = f'{row[start_index]}'
        parts = [(header[index], value) for index, value in enumerate(row) if len(value.strip()) != 0][start_index+1:]
        text = f'{text}\'s {self.clean_predicate(parts[0][0])} {self.append_value(parts[0][1])}'
        for part in parts[1:-1]:
            # if empty or len(self.clean_predicate(part[0])) > 0:
            text = f'{text}, its {self.clean_predicate(part[0])} {self.append_value(part[1])}'
        # if empty or len(self.clean_predicate(parts[-1][0])) > 0:
        text = f'{text}, and its {self.clean_predicate(parts[-1][0])} {self.append_value(parts[-1][1])}.'
        return text

    def table_2_text(self, csv_path: str, empty=False) -> list:
        header, rows = self.read_csv(csv_path)
        sep = '\t' if csv_path[-4:] == '.tsv' else ','
        extra_info = self.append_aggregation_info(pd.read_csv(csv_path, sep=sep))

        #  maybe make a branch here, if extra_info is larger than 500 or not, and handle it diffently
        if len(extra_info.split()) > 500:
            extra_info = self.splitExtraInfoInArraysOfSuitableSize(extra_info)
        else:
            extra_info = [extra_info]

        # okay so I have extra_info split but now I have to deal with the context itself, maybe its better to separate text from extra info in runs?
        resultArray = []
        allText = ''
        for row in rows:
            rowText = self.row_2_text(row, header, empty)
            test = allText + '\n' + rowText + '\n'
            if len(test.split()) > 500:
                extra_info.append(allText+ '\n')
                allText = rowText
                continue
            allText = allText + '\n' + rowText
        extra_info.append(allText)

        if extra_info[0] == '':
            print('hey')

        if len(extra_info) == 2 and len((extra_info[0]+extra_info[1]).split()) < 500:
            return [(extra_info[0]+extra_info[1])]
        else:
            # attention here, i might have fucked up and never appended the last string
            # might be cool to make a function which combines all the chunks in the maximum size
            # resultArray[-1] = resultArray[-1] + '\n' + extra_info
            return extra_info

    def splitExtraInfoInArraysOfSuitableSize(self, extra_info):
        lineBreaksIndexesForCorrectSplit = [pos for pos, char in enumerate(extra_info) if char == '\n']
        numberOfArrays = 1

        startingIndexForComparisson = 0
        lastCharBelowCeiling = 0
        returningArrays = []
        for pos, char in enumerate(lineBreaksIndexesForCorrectSplit):
            if len(extra_info[startingIndexForComparisson:char].split()) < 500:
                lastCharBelowCeiling = char
                continue
            else:
                returningArrays.append(extra_info[startingIndexForComparisson:lastCharBelowCeiling])
                numberOfArrays = numberOfArrays+1
                startingIndexForComparisson = lastCharBelowCeiling+1
        returningArrays.append(extra_info[startingIndexForComparisson:])
        return returningArrays


if __name__ == '__main__':
    t2t = T2T()
    print(t2t.table_2_text("./datasets/TabMCQ/csv/monarch-65.tsv", empty=True))
