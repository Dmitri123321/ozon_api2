import json
from pandas.io.json import json_normalize
import openpyxl

# file = open('text.json', 'r')
# text = file.read()
# file.close()
# text = json.loads(text)
text = [{"writer": "Mark Ross",
      "nationality": "USA",
      "books": [
          {"title": "XML Cookbook", "price": 23.56},
          {"title": "Python Fundamentals", "price": 50.70},
          {"title": "The NumPy library", "price": 12.30}
      ]
      },
     {"writer": "Barbara Bracket",
      "nationality": "UK",
      "books": [
          {"title": "Java Enterprise", "price": 28.60},
          {"title": "HTML5", "price": 31.35},
          {"title": "Python for Dummies", "price": 28.00}
      ]
      }]

frame = json_normalize(text, 'books', ['nationality', 'writer'])
frame.to_excel('test.xlsx')
