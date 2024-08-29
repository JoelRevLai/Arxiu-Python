from flask import Flask, request, jsonify
from pymongo import MongoClient
from flask_cors import CORS
from dotenv import load_dotenv
import os
from datetime import datetime
import re

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Conexión a MongoDB Atlas
mongo_uri = os.getenv("MONGO_URI")
client = MongoClient(mongo_uri)
db = client["archivo_digital"]
collection = db["bilbiografia_1.0"]

@app.route('/')
def home():
    return "API is running!"


def serialize_document(doc):
    if '_id' in doc:
        doc['_id'] = str(doc['_id'])
    if 'year' in doc and 'month' in doc and 'day' in doc:
        doc['date'] = f"{doc['day']:02d}/{doc['month']:02d}/{doc['year']}"
    elif 'year' in doc and 'month' in doc:
        doc['date'] = f"{doc['month']:02d}/{doc['year']}"
    elif 'day' in doc and 'month' in doc:
        doc['date'] = f"{doc['day']:02d}/{doc['month']:02d}"
    elif 'year' in doc:
        doc['date'] = f"{doc['year']}"
    else:
        doc['date'] = None
    if 'authors' in doc:
        doc['authors'] = [{'name': author} for author in doc['authors']]
    else:
        doc['authors'] = None
    for role in ['editor', 'translator', 'illustrator', 'coordinator', 'director']:
        if role in doc:
            doc[role] = [{'name': person} for person in doc[role]]
        else:
            doc[role] = None
    if 'pages' in doc:
        pass
    else:
        doc['pages'] = None
    if 'city' in doc:
        pass
    else:
        doc['city'] = None
    return doc

@app.route('/search', methods=['GET'])
def search():
    try:
        query = request.args.get('query', '')
        optional_query = request.args.get('optionalQuery', '')
        source = request.args.get('source')
        type_ = request.args.get('type')
        publisher = request.args.get('publisher', '')
        start_date = request.args.get('startDate')
        end_date = request.args.get('endDate')
        city = request.args.get('city')
        user_selected_fields = request.args.get('fields', "")
        user_selected_fields2 = request.args.get('fields2', "")
        search_type = request.args.get('searchTypeValue', '').lower()
        sort_by = request.args.get('sortBy', 'relevance')  # Valor por defecto

        page = request.args.get('page', 1)  # valor por defecto 1
        try:
            page = int(page)
        except ValueError:
            page = 1  # Si 'page' no es un número válido, establece a 1 como valor predeterminado
        
        limit = request.args.get('limit', 10)
        try:
            limit = int(limit)
        except ValueError:
            limit = 10  # Si 'limit' no es un número válido, establece un valor predeterminado

        skip = (page - 1) * limit

        print(f"Received query: {query}")
        print(f"Received optional_query: {optional_query}")
        print(f"Received search_type: {search_type}")
        print(f"Received source: {source}")
        print(f"Received type: {type_}")
        print(f"Received publisher: {publisher}")
        print(f"Received start_date: {start_date}")
        print(f"Received end_date: {end_date}")
        print(f"Received city: {city}")
        print(f"Received fields: {user_selected_fields}")
        print(f"Received fields2: {user_selected_fields2}")

        def build_search_stage(query, optional_query=None, search_type='', user_fields=None, user_fields2=None):
            default_fields = ["title", "source", "authors", "editor", "folder_names", "publisher", "translator", "illustrator", "coordinator", "director", "city", "book"]
            fields = user_fields if user_fields else default_fields
            fields2 = user_fields2 if user_fields2 else default_fields
            if query.startswith('"') and query.endswith('"'):
                query_type = "phrase"
                query = query.strip('"')
            else:
                query_type = "text"

            if optional_query and optional_query.startswith('"') and optional_query.endswith('"'):
                optional_query_type = "phrase"
                optional_query = optional_query.strip('"')
            else:
                optional_query_type = "text"

            def build_compound(query, optional_query, query_type, optional_query_type, compound_type):
                stages = [
                    {
                        query_type: {
                            "query": query,
                            "path": fields
                        }
                    }
                ]
                if query_type == "text":
                    stages[0][query_type]["fuzzy"] = {"maxEdits": 1}
                stages.append(
                    {
                        optional_query_type: {
                            "query": optional_query,
                            "path": fields2
                        }
                    }
                )
                if optional_query_type == "text":
                    stages[1][optional_query_type]["fuzzy"] = {"maxEdits": 1}
                
                return {
                    "$search": {
                        "index": "default",
                        "compound": {
                            compound_type: stages
                        }
                    }
                }

            if search_type == 'and' and optional_query:
                return build_compound(query, optional_query, query_type, optional_query_type, "must")
            elif search_type == 'or' and optional_query:
                return build_compound(query, optional_query, query_type, optional_query_type, "should")
            elif search_type == 'mustnot' and optional_query:
                stages = [
                    {
                        query_type: {
                            "query": query,
                            "path": fields
                        }
                    }
                ]
                if query_type == "text":
                    stages[0][query_type]["fuzzy"] = {"maxEdits": 1}
                stages.append(
                    {
                        optional_query_type: {
                            "query": optional_query,
                            "path": fields2
                        }
                    }
                )
                if optional_query_type == "text":
                    stages[1][optional_query_type]["fuzzy"] = {"maxEdits": 1}
                
                return {
                    "$search": {
                        "index": "default",
                        "compound": {
                            "must": [stages[0]],
                            "mustNot": [stages[1]]
                        }
                    }
                }
            else:
                search_stage = {
                    "$search": {
                        "index": "default",
                        query_type: {
                            "query": query,
                            "path": fields
                        }
                    }
                }
                if query_type == "text":
                    search_stage["$search"][query_type]["fuzzy"] = {"maxEdits": 1}
                return search_stage

        query_pipeline = []

        if query or optional_query:
            search_stage = build_search_stage(query, optional_query, search_type, user_selected_fields, user_selected_fields2)
            query_pipeline.append(search_stage)

        match_filter = {}
        if source:
            match_filter["source"] = source
        if type_:
            match_filter["type"] = type_
        if publisher:
            match_filter['publisher'] = publisher
        if city:
            match_filter['city'] = city

        if start_date or end_date:
            if start_date and re.match(r'^\d{4}$', start_date):
                start_date = f'01/01/{start_date}'
            if end_date and re.match(r'^\d{4}$', end_date):
                end_date = f'31/12/{end_date}'

            start_datetime = datetime.strptime(start_date, '%d/%m/%Y') if start_date else None
            end_datetime = datetime.strptime(end_date, '%d/%m/%Y') if end_date else None

            date_filter = {"$and": []}

            if start_datetime:
                date_filter["$and"].append({
                    "$expr": {
                        "$gte": [
                            {"$dateFromParts": {
                                "year": "$year",
                                "month": {"$ifNull": ["$month", 1]},
                                "day": {"$ifNull": ["$day", 1]}
                            }},
                            {"$literal": start_datetime}
                        ]
                    }
                })

            if end_datetime:
                date_filter["$and"].append({
                    "$expr": {
                        "$lte": [
                            {"$dateFromParts": {
                                "year": "$year",
                                "month": {"$ifNull": ["$month", 12]},
                                "day": {"$ifNull": ["$day", 31]}
                            }},
                            {"$literal": end_datetime}
                        ]
                    }
                })

            if date_filter["$and"]:
                query_pipeline.append({"$match": date_filter})

        if match_filter:
            query_pipeline.append({"$match": match_filter})

        # Filtro para excluir documentos cuya fecha/publisher sea "N/D"
        if start_date or end_date:
            query_pipeline.append({"$match": {"year": {"$ne": None}}})
        if publisher:
            query_pipeline.append({"$match": {"publisher": {"$ne": None}}})

        # Añadir campos normalizados para ordenación
        query_pipeline.append({
    "$addFields": {
        "normalized_date": {
            "$cond": {
                "if": { "$and": [
                    { "$ne": ["$year", None] },
                    { "$ne": ["$month", None] },
                    { "$ne": ["$day", None] }
                ]},
                "then": {
                    "$dateFromParts": {
                        "year": "$year",
                        "month": {"$ifNull": ["$month", 1]},
                        "day": {"$ifNull": ["$day", 1]}
                    }
                },
                "else": None  # Asignar None si no hay fecha completa
            }
        }
    }
})
        

       # Agregar etapa para contar el total de documentos que coinciden con el filtro
        count_pipeline = query_pipeline.copy()
        count_pipeline.append({'$count': 'total_documents'})

        # Ejecutar el pipeline para obtener el total de documentos
        total_documents_result = list(collection.aggregate(count_pipeline))
        total_documents = total_documents_result[0]['total_documents'] if total_documents_result else 0
        
        print(match_filter)
        print(query_pipeline)

        # Ordenar usando el nuevo campo sort_date
        def get_sort_order(sort_by):
            if sort_by == 'date':
                query_pipeline.append({
                    "$addFields": {
                        "sort_date": {
                            "$ifNull": ["$normalized_date", {
                                "$dateFromParts": {
                                    "year": 1111,
                                    "month": 12,
                                    "day": 31
                                }
                            }]
                        }
                    }
                })
                return [
                    ('sort_date', -1),  # Ordenar por fecha descendente
                    ('_id', 1)  # Desempate para mantener un orden estable
                ]
            elif sort_by == 'date-asc':
                query_pipeline.append({
                    "$addFields": {
                        "sort_date": {
                            "$ifNull": ["$normalized_date", {
                                "$dateFromParts": {
                                    "year": 9999,
                                    "month": 12,
                                    "day": 31
                                }
                            }]
                        }
                    }
                })
                return [
                    ('sort_date', 1),  # Ordenar por fecha ascendente, los null se van al final
                    ('_id', 1)  # Desempate para mantener un orden estable
                ]
            elif sort_by in ['title', 'title-desc']:
                # Normalizar letras mayúsculas con tildes cerradas y abiertas en el campo `normalized_title`
                query_pipeline.append({
                    "$addFields": {
                        "normalized_title": {
                            "$replaceAll": {
                                "input": {
                                    "$replaceAll": {
                                        "input": {
                                            "$replaceAll": {
                                                "input": {
                                                    "$replaceAll": {
                                                        "input": {
                                                            "$replaceAll": {
                                                                "input": {
                                                                    "$replaceAll": {
                                                                        "input": {
                                                                            "$replaceAll": {
                                                                                "input": {
                                                                                    "$replaceAll": {
                                                                                        "input": {
                                                                                            "$replaceAll": {
                                                                                                "input": {
                                                                                                    "$replaceAll": {
                                                                                                        "input": {
                                                                                                            "$replaceAll": {
                                                                                                                "input": "$title",
                                                                                                                "find": "Á",
                                                                                                                "replacement": "A"
                                                                                                            }
                                                                                                        },
                                                                                                        "find": "À",
                                                                                                        "replacement": "A"
                                                                                                    }
                                                                                                },
                                                                                                "find": "É",
                                                                                                "replacement": "E"
                                                                                            }
                                                                                        },
                                                                                        "find": "È",
                                                                                        "replacement": "E"
                                                                                    }
                                                                                },
                                                                                "find": "Í",
                                                                                "replacement": "I"
                                                                            }
                                                                        },
                                                                        "find": "Ì",
                                                                        "replacement": "I"
                                                                    }
                                                                },
                                                                "find": "Ó",
                                                                "replacement": "O"
                                                            }
                                                        },
                                                        "find": "Ò",
                                                        "replacement": "O"
                                                    }
                                                },
                                                "find": "Ú",
                                                "replacement": "U"
                                            }
                                        },
                                        "find": "Ù",
                                        "replacement": "U"
                                    }
                                },
                                "find": "Ñ",
                                "replacement": "N"
                            }
                        }
                    }
                })

                # Usar regex para encontrar el primer carácter alfabético y hacer la ordenación a partir de él
                query_pipeline.append({
                    "$addFields": {
                        "normalized_title": {
                            "$cond": {
                                "if": {"$regexMatch": {"input": "$normalized_title", "regex": r"[a-zA-Z].*"}},
                                "then": {
                                    "$regexFind": {
                                        "input": "$normalized_title",
                                        "regex": r"[a-zA-Z].*"
                                    }
                                },
                                "else": "$normalized_title"
                            }
                        }
                    }
                })

                return [
                    ('normalized_title.match', 1 if sort_by == 'title' else -1)
                ]
            else:
                return [('relevance', 1)]  # Valor por defecto
        # Obtener la ordenación
        sort_order = get_sort_order(sort_by)
        query_pipeline.append({'$sort': dict(sort_order)})

        # Etapas de paginación
        query_pipeline.append({'$skip': skip})
        query_pipeline.append({'$limit': limit})

       # Aplicar paginación y limitar resultados
        results = list(collection.aggregate(query_pipeline))

        serialized_results = [serialize_document(doc) for doc in results]

        response = {
            "total_documents": total_documents,
            "total_pages": (total_documents + limit - 1) // limit,  # Redondear hacia arriba
            "current_page": page,
            "results": serialized_results
        }

        return jsonify(response)

    except Exception as e:
        print(f"Exception: {e}")
        return jsonify({"message": "Error during search", "error": str(e)}), 500

@app.route('/get_sources', methods=['GET'])
def get_sources():
    try:
        sources = collection.distinct("source")
        return jsonify(sources)
    except Exception as e:
        return jsonify({"message": "Error retrieving sources", "error": str(e)}), 500

@app.route('/get_types', methods=['GET'])
def get_types():
    try:
        types = collection.distinct("type")
        return jsonify(types)
    except Exception as e:
        return jsonify({"message": "Error retrieving types", "error": str(e)}), 500

@app.route('/get_publishers', methods=['GET'])
def get_publishers():
    publishers = collection.distinct("publisher")
    return jsonify(publishers)

@app.route('/get_city', methods=['GET'])
def get_city():
    try:
        city = collection.distinct("city")
        return jsonify(city)
    except Exception as e:
        return jsonify({"message": "Error retrieving cities", "error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
