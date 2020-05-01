function(){
    emotion = this.Emotion;
    this.Words.forEach(function(word) {
        emit({"Emotion":emotion, "Word": word}, 1);
    });
}