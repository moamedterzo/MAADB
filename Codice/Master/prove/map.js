function() {

    var values = this.Text.split(" ");

    for (var i = 0; i < values.length; i++) {
        emit(values[i], 1);
    }
}