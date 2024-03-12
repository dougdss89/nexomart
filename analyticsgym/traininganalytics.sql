CREATE SCHEMA [person]
GO

CREATE SCHEMA [food]
GO

CREATE SCHEMA [training]
GO

CREATE TABLE [person].[customer] (
  [userid] int,
  [username] text,
  [age] int,
  [birthdate] date,
  [gender] text,
  [weight] decimal,
  [personhashid] text
)
GO

CREATE TABLE [person].[usermeasure] (
  [usermeasureid] int,
  [userid] int,
  [height] decimal,
  [arms] decimal,
  [forearms] decimal,
  [shoulder] decimal,
  [measurehashid] text
)
GO

CREATE TABLE [food].[food_data] (
  [foodid] int,
  [foodname] text,
  [foodcategoryid] int,
  [preparationid] int,
  [servesid] int,
  [foodhashid] text
)
GO

CREATE TABLE [food].[preparationmode] (
  [prepmodeid] int,
  [prepmode] text,
  [prepmodehashid] text
)
GO

CREATE TABLE [food].[serves] (
  [servesid] int,
  [servestype] text,
  [portion] int
)
GO

CREATE TABLE [food].[dailydiet] (
  [dailydietid] int,
  [dailyfoodid] int,
  [dietdate] data,
  [quantity] decimal,
  [carbs] decimal,
  [protein] decimal,
  [fat] decimal,
  [sodium] decimal,
  [calories] decimal
)
GO

CREATE TABLE [training].[workout] (
  [workoutid] int PRIMARY KEY,
  [workoutexerciseid] int,
  [workoutdate] date,
  [userid] int,
  [exerciseid] int,
  [exerciseorder] smallint,
  [exerciseweight] decimal,
  [workoutquantity_one] int,
  [repetionquantity_two] decimal,
  [repetionquantity_three] decimal,
  [repetionquantity_four] decimal,
  [repetionquantity_five] decimal,
  [workoutrest1] time,
  [workoutrest2] time,
  [workoutrest3] time,
  [workoutrest4] time,
  [workouthashid] text
)
GO

CREATE TABLE [training].[advancedtechnique] (
  [advancedworkoutid] int PRIMARY KEY,
  [advtechniqueid] int,
  [workoutid] int,
  [exerciseid] int,
  [advancedrepqtd_one] int,
  [advancedrepqtd_two] int,
  [advancedrepqtd_three] int,
  [advancedrepwght_one] decimal,
  [advancedrepwght_two] decimal,
  [advancedrepwght_three] decimal,
  [advancetechniquehashid] text
)
GO

CREATE TABLE [training].[exercise] (
  [exerciseid] int,
  [exercisename] text,
  [exercisecategoryid] int,
  [hashid] text
)
GO

CREATE TABLE [training].[advtechniquetype] (
  [techniqueid] int,
  [techniquename] text,
  [advancedtypehashid] text
)
GO

CREATE TABLE [training].[exercisecategory] (
  [exercisecategoryid] int,
  [categoryname] text,
  [categoryhashid] text,
  [exercisecategoryhashid] text
)
GO

ALTER TABLE [training].[exercise] ADD FOREIGN KEY ([exercisecategoryid]) REFERENCES [training].[exercisecategory] ([exercisecategoryid])
GO

ALTER TABLE [training].[workout] ADD FOREIGN KEY ([exerciseid]) REFERENCES [training].[exercise] ([exerciseid])
GO

ALTER TABLE [training].[workout] ADD FOREIGN KEY ([userid]) REFERENCES [person].[customer] ([userid])
GO

ALTER TABLE [training].[advancedtechnique] ADD FOREIGN KEY ([advtechniqueid]) REFERENCES [training].[advtechniquetype] ([techniqueid])
GO

ALTER TABLE [training].[advancedtechnique] ADD FOREIGN KEY ([workoutid]) REFERENCES [training].[workout] ([workoutid])
GO

ALTER TABLE [training].[advancedtechnique] ADD FOREIGN KEY ([exerciseid]) REFERENCES [training].[exercise] ([exerciseid])
GO

ALTER TABLE [food].[food_data] ADD FOREIGN KEY ([servesid]) REFERENCES [food].[serves] ([servesid])
GO

ALTER TABLE [food].[food_data] ADD FOREIGN KEY ([preparationid]) REFERENCES [food].[preparationmode] ([prepmodeid])
GO

ALTER TABLE [person].[usermeasure] ADD FOREIGN KEY ([userid]) REFERENCES [person].[customer] ([userid])
GO

ALTER TABLE [food].[dailydiet] ADD FOREIGN KEY ([dailyfoodid]) REFERENCES [food].[food_data] ([foodid])
GO
