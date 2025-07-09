[
    {
        '$match': {
            '$expr': {
                '$lte': [
                    {
                        '$strLenCP': '$content'
                    }, 5
                ]
            }
        }
    }
]