import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

players = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))

teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))

titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic.append(dict(r))


class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None


import copy


class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table

    def join(self, other_table, common_key):
        joined_table = Table(
            self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table

    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table

    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            temps.append(float(item1[aggregation_key]))
        return function(temps)

    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)

#
# table1 = Table('cities', cities)
# table2 = Table('countries', countries)
# my_DB = DB()
# my_DB.insert(table1)
# my_DB.insert(table2)
# my_table1 = my_DB.search('cities')

# print("Test filter: only filtering out cities in Italy")
# my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
# print(my_table1_filtered)
# print()
#
# print("Test select: only displaying two fields, city and latitude, for cities in Italy")
# my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
# print(my_table1_selected)
# print()
#
# print("Calculting the average temperature without using aggregate for cities in Italy")
# temps = []
# for item in my_table1_filtered.table:
#     temps.append(float(item['temperature']))
# print(sum(temps)/len(temps))
# print()
#
# print("Calculting the average temperature using aggregate for cities in Italy")
# print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
# print()
#
# print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
# my_table2 = my_DB.search('countries')
# my_table3 = my_table1.join(my_table2, 'country')
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
# print(my_table3_filtered.table)
# print()
# print("Selecting just three fields, city, country, and temperature")
# print(my_table3_filtered.select(['city', 'country', 'temperature']))
# print()
#
# print("Print the min and max temperatures for cities in EU that do not have coastlines")
# my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
# print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
# print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
# print()
#
# print("Print the min and max latitude for cities in every country")
# for item in my_table2.table:
#     my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
#     if len(my_table1_filtered.table) >= 1:
#         print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
# print()

player_data  = Table('player', players)
player_data_team_ia = player_data.filter(lambda x: "ia" in x['team'])
ia_passes = player_data_team_ia.filter(lambda x: int(x['passes']) > 100)
ia_passes_play = ia_passes.filter(lambda x: int(x['minutes']) < 200)
player_that_we_want = ia_passes_play.select(['surname', 'team', 'position'])
print("Player on a team with ia that played less than 200 minutes and made more than 100 passes.")
print(player_that_we_want)
print()
team_data = Table('team', teams)
team_data_rank_under_10 = team_data.filter(lambda x: int(x['ranking']) < 10)
team_data_rank_under_10_game = team_data_rank_under_10.select(['games'])
sum = 0
for x in team_data_rank_under_10_game:
    sum += int(x['games'])
print(f"Average number of games played for teams ranking below 10: {sum/len(team_data_rank_under_10_game)}")
team_data_rank_above_10 = team_data.filter(lambda x: int(x['ranking']) >= 10)
team_data_rank_above_10_game = team_data_rank_above_10.select(['games'])
sum1 = 0
for i in team_data_rank_above_10_game:
    sum1 += int(i['games'])
print(f"Average number of games played for teams ranking above or equal 10: {sum1/len(team_data_rank_above_10_game)}")

midfielders = player_data.filter(lambda x: x['position'] == 'midfielder')
forwards = player_data.filter(lambda x: x['position'] == 'forward')
midfielders_passes_data = midfielders.select(['passes'])
forwards_passes_data = forwards.select(['passes'])
sum_mid_pass = 0
sum_for_pass = 0
for y in midfielders_passes_data:
    sum_mid_pass += int(y['passes'])
for k in forwards_passes_data:
    sum_for_pass += int(k['passes'])
print(f"Average number of passes made by forwards : {sum_for_pass/len(forwards_passes_data)}")
print(f"Average number of passes made by midfielders : {sum_mid_pass/len(midfielders_passes_data)}")
print()
titanic_data = Table('Titanic', titanic)
first_class = titanic_data.filter(lambda x: int(x['class']) == 1)
third_class  = titanic_data.filter(lambda x: int(x['class']) == 3)
first_class_fare = first_class.select(['fare'])
third_class_fare = third_class.select(['fare'])
sum_first_fare = 0
sum_third_fare = 0
for h in first_class_fare:
    sum_first_fare += float(h['fare'])
for j in third_class_fare:
    sum_third_fare += float(j['fare'])
print(f"Average fare paid by passengers in the first class : {sum_first_fare/len(first_class_fare)}")
print(f"Average fare paid by passengers in the third class : {sum_third_fare/len(third_class_fare)}")
print()
male = titanic_data.filter(lambda x: x['gender'] == 'M')
female = titanic_data.filter(lambda x: x['gender'] == 'F')
survival_rate_male = male.select(['survived'])
survival_rate_female = female.select(['survived'])
sum_survived_male = 0
sum_survived_female = 0
for b in survival_rate_male:
    if b['survived'] == 'yes':
        sum_survived_male += 1
for o in survival_rate_female:
    if o['survived'] == 'yes':
        sum_survived_female += 1
print(f"Survival rate of male : {(sum_survived_male/len(survival_rate_male)) * 100} %")
print(f"Survival rate of female : {(sum_survived_female/len(survival_rate_female)) * 100} %")
