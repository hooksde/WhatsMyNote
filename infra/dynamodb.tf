resource "aws_dynamodb_table" "note_events" {
  name         = "note-events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "sourceId"
  range_key    = "timestamp"

  attribute {
    name = "sourceId"
    type = "S"
  }
  attribute {
    name = "timestamp"
    type = "N"
  }
}

resource "aws_dynamodb_table" "chord_events" {
  name         = "chord-events"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "sourceId"
  range_key    = "startTs"

  attribute {
    name = "sourceId"
    type = "S"
  }
  attribute {
    name = "startTs"
    type = "N"
  }
}
