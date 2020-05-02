db.collection.aggregate([
    { $unwind : "$hashtag" },
    {
     $group:
       {
         _id: {emotion: "$emotion", hashtag: "$hashtag"} ,
         count: { $sum: 1 }
       }
    }
])