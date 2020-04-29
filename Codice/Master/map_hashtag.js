function(){
    emotion = this.Emotion;
    this.Hashtag.forEach(function(hashtag) {
        emit({"Emotion":emotion, "Hashtag": hashtag}, 1);
    });
}