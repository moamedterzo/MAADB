function(){
    emotion = this.Emotion;
    this.Emoticon.forEach(function(emoticon) {
        emit({"Emotion":emotion, "Emoticon": emoticon}, 1);
    });
}