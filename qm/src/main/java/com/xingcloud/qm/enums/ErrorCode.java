package com.xingcloud.qm.enums;

public enum ErrorCode {
  // Unknown exception
  ERR_1,
  // Parameter exceptions
  // Project id is null
  ERR_10,
  // No parameters
  ERR_11,
  // Invalid json format
  ERR_12,
  // Invalid query container
  ERR_1275,
  // Invalid number of day
  ERR_12751,

  // Date and segment exceptions
  // Begin date or end date is null
  ERR_20,
  // Could not split date
  ERR_21,
  // Could not init parameters
  ERR_22,
  // Could not parse date
  ERR_2012,

  // Query is interrupted by placeholder
  ERR_33,
  // Cannot parse incremental
  ERR_34,
  // Ranging exception
  ERR_35,
  // UI table does not exist in redis
  ERR_36,
  // Query exceptions
  // QueryException: The most common error in DataDriller
  // Tribute to a famous error occurs in Diablo III -
  // Error 37: The servers are busy at this time. Please try again later.
  ERR_37,
  // Time out
  ERR_3007,
  // Mongo exception
  ERR_38,
  // Total/Natural summary combination exception
  ERR_39

}
