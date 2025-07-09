[
    {
        '$addFields': {
            'date': {
                '$dateTrunc': {
                    'date': {
                        '$dateFromString': {
                            'dateString': '$at', 
                            'format': '%Y-%m-%d %H:%M:%S'
                        }
                    }, 
                    'unit': 'day'
                }
            }
        }
    }, {
        '$project': {
            'date': 1, 
            'score': 1
        }
    }, {
        '$group': {
            '_id': '$date', 
            'avg_rating': {
                '$avg': '$score'
            }
        }
    }, {
        '$project': {
            'date': '$_id', 
            'avg_rating': 1, 
            '_id': 0
        }
    }, {
        '$sort': {
            'date': 1
        }
    }
]