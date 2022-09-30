import csv
from pandas.io.json import json_normalize
import openpyxl
import json


class CSV:
    def __init__(self):
        self.path = ''

    def _get_lines(self, items):
        lines = [pair.get('offer_id') + ';' + pair.get('name') + ';' + pair.get('price') for pair in items]
        return lines

    def write_csv(self, items, file_name):
        self.path = '../results/' + file_name
        lines = self._get_lines(items)
        self._write_lines_to_csv(lines)

    def _write_lines_to_csv(self, lines):
        with open(self.path, "a", encoding='utf-8-sig', newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=';', quoting=csv.QUOTE_NONE, quotechar='"', escapechar='"')
            for line in lines:
                try:
                    writer.writerow([line])
                except Exception as e:
                    print(e)
                    continue


class XLSX:
    def __init__(self):
        self.path = '../results/'

    def price_xls(self, items, file_name):
        path = self.path + file_name
        items = {'items': [{'offer_id': pair.get('offer_id'), 'name': pair.get('name'), 'price': pair.get('price')} for pair in items]}
        frame = json_normalize(items, 'items')
        frame.to_excel(f'{path}')

    def stock_xls(self, items, file_name):
        path = self.path + file_name
        items = {'items': [{'offer_id': pair.get('offer_id'), 'product_id': pair.get('product_id'),
                            'fbo_present': pair.get('stocks')[0].get('present'),
                            'fbo_reserved': pair.get('stocks')[0].get('reserved'),
                            'fbs_present': pair.get('stocks')[0].get('present'),
                            'fbs_reserved': pair.get('stocks')[0].get('reserved')} for pair in items]}
        frame = json_normalize(items, 'items')
        frame.to_excel(f'{path}')

    def ordered_xls(self, items, file_name):
        path = self.path + file_name
        items = {'items': [{'id': pair.get('dimensions')[0]['id'], 'name': pair.get('dimensions')[0]['name'],
                            'ordered': pair.get('metrics')[0]} for pair in items]}
        frame = json_normalize(items, 'items')
        frame.to_excel(f'{path}')


class JSON_:
    def __init__(self):
        self.path = '../results/'

    def write_json(self, items, file_name):
        path = self.path + file_name
        try:
            with open(path, 'w', encoding='utf-8') as file:
                json.dump(items, file, indent=4, ensure_ascii=False)
        except Exception as e:
            print(e)
