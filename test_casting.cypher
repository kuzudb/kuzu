// casting floating point to int
return cast(2.49 as int32); // result: 2
return cast(2.51 as int32); // 3
return cast(2.5 as int32); // 2
return cast(3.5 as int32); // 4
return cast(0.5 as int32); // 0
return cast(1.5 as int32); // 2
return cast(-0.5 as int32); // 0

// overflow
return cast(255 as int8); // error
return cast(-1000 as int8); // error

// casting signed int to unsigned int
return cast(127 as uint8); // 127
return cast(-1 as uint8); // error
return cast(-128 as uint8); // error
