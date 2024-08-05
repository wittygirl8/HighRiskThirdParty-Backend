import base64
import traceback
import pandas as pd

from utils.db import MSSQLConnection

db = MSSQLConnection()
from flask import current_app as app

class User:

    def login(self, data):
        try:
            username = data.get("username")
            app.logger.info(data.get("password"))
            # password = base64.b64decode(data.get("password")).decode("utf-8")
            password = data.get("password")
            df = pd.read_csv('data/app.user.csv')
            print(df)
            user = df[(df['username'] == username) &
                      (df['password'] == password) &
                      (df['isActive'] == 1)]
            print(user)
            print(username)
            print(password)
            # user = [{'username': username, 'password':password,'name':'admin','type':'admin','isActive':1}]
            if not user.empty:
                user = user.iloc[0].to_dict()
                country = []
                if "type" in user and user["type"] == "admin":
                    pass
                else:
                    access_df = pd.read_csv('data/app.access.csv')
                    country_df = pd.read_csv('data/app.country.csv')
                    merged_df = pd.merge(access_df, country_df, left_on='country_id', right_on='id', how='left')
                    print(merged_df)
                    user_access_df = merged_df[merged_df['user_id'] == user['id']]
                    print(user_access_df)
                    country = user_access_df['code'].tolist()
                    print('list', country)
                user["country"] = country
                return user
        except Exception as e:
            print("Features.case_login(): " + str(e))
            traceback.print_exc()
            return None

    @staticmethod
    def get_claims(data):
        claims = []
        return claims

    def create(self, data):  # tbd
        try:

            return True, 'User Created successfully', {}
        except Exception as e:
            print("User.create(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"

    def get_all(self, data):
        try:
            query = """
                select id, 
                    username, 
                    type, 
                    name, 
                    email, 
                    phone, 
                    isActive, 
                    updatedBy, 
                    createdOn, 
                    lastLoggedIn 
                from [app].[user]
            """
            users = db.select(query)
            return True, "All portal users", users
        except Exception as e:
            print("User.get_all(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"

    def update(self, data):  # tbd
        try:
            # YOUR CODE HERE
            return True, 'User updated successfully', {}
        except Exception as e:
            print("User.update(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"

    def delete(self, data):
        try:
            if "user_id" not in data and not data["user_id"]:
                return False, "Input error"
            _ret = db.exec(f"delete from [app].[user] WHERE user_id=?", data["user_id"])
            if not _ret:
                raise Exception("Something went wrong")
            return True, 'User deleted successfully', {}
        except Exception as e:
            print("User.delete(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"

    def status_update(self, data):
        try:
            if "user_id" not in data and not data["user_id"] and "status" not in data:
                return False, "Input error"
            _ret = db.exec(f"UPDATE [app].[user] SET isActive = ? WHERE id = ?", data["status"], data["user_id"])
            if not _ret:
                raise Exception("Something went wrong")
            return True, 'User updated successfully', {}
        except Exception as e:
            print("User.status_update(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"
