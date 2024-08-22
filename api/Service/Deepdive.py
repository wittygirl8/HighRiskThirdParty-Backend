import traceback
import os, json
from utils.db import MSSQLConnection
import pandas as pd

db = MSSQLConnection()
import datetime


class Deepdive:
    def __init__(self):
        self.base_path = "data"

    def read_json(self, filename):
        json_object = dict()
        try:
            if os.path.isfile(self.base_path + "\\" + filename):
                with open(self.base_path + "\\" + filename, 'r', encoding='utf-8') as openfile:
                    # Reading from json file
                    json_object = json.load(openfile)
        except Exception as e:
            print(e)
        finally:
            return json_object

    def get_countries(self, data):  # tbd
        try:
            user = data["user"]
            if user["type"].strip() != "admin":
                print("in if")
                print("in if")
                access_df = pd.read_csv('data/app.access.csv')
                country_df = pd.read_csv('data/app.country.csv')
                merged_df = pd.merge(access_df, country_df, left_on='country_id', right_on='id', how='inner')
                user_access_df = merged_df[merged_df['user_id'] == user['id']]
                user = user_access_df[['id', 'name', 'code']]
                users = user.rename(columns={'name': 'country'})
            else:
                print("in else")
                country_df = pd.read_csv('data/app.country.csv')
                users = country_df[['id', 'name', 'code']]
                users = json.loads(users.to_json(orient='records'))
            print(users)
            return True, "access countries", users
        except Exception as e:
            print("Deepdive.data_by_country(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"

    def get_negative_news(self, title, hco_news, hcp_news, color):

        if color == "#fb7e81":
            for article in hco_news:
                if (article['hco'] == title) and (
                        article["sentiment"].lower() == "negative." or article["sentiment"].lower() == "negative"):
                    return True

        if color == "#95c0f9":
            for article in hcp_news:

                if (article['hcp'] == title) and (
                        article["sentiment"].lower() == "negative." or article["sentiment"].lower() == "negative"):
                    return True

        return False

    def graph_by_country(self, data):
        """ Graph with selectable org type. If only HCOs or HCPs are selected,
            pairs of edges are joined between them if there is a common middle node """
        try:
            print("data________", data)
            country = data['country']
            conn = data.get('connection')
            link = data.get('link')
            org = data.get('orgType')
            if country == 'null':
                return True, "graph_by_country", []

            # Read the CSV data into a DataFrame
            file_path = 'data/app2.AllNodes.csv'
            df = pd.read_csv(file_path)

            print("vAllNodes________", df)

            # get payment range
            currency_mapping = {'usa': 'USD', 'brazil': 'BRL', 'spain': 'EUR'}
            # set default minimum to 1 and default to no max
            payment_min = data['min'] if (data['min'] not in ('0', 'null')) else 1  # (10000 if country == 'usa' else 5000)
            payment_max = data['max'] if (data['max'] not in ('0', 'null')) else None

            print("payment_max", payment_max)
            # query nodes and edges
            edgeSource = 'vAllEdges' if conn == 'weak' else 'vStrongEdges'

            # get all nodes from selected country with amount and indicator for payment range

            # condition for inPaymentRange; add 1 to capture fractions
            # query_ext = f' and PaymentAmount <= ({payment_max} + 1)' if payment_max is not None else ''

            # condition for node type - HCO ids start with a country initial
            if org == 'hco':
                # org_ext = f" and LEFT([ID], 1) IN ('B', 'S', 'U')"
                org_ext = df['ID'].str[0].isin(['B', 'S', 'U'])
            elif org == 'hcp':
                org_ext = ~df['ID'].str[0].isin(['B', 'S', 'U'])
                # org_ext = f" and LEFT([ID], 1) NOT IN ('B', 'S', 'U')"
            else:
                org_ext = ''  # No additional filter

            # query = f"select [id] as [node_id], [Name] as VendorName, PaymentAmount, InteractionCount, \
            #           case when PaymentAmount >= {payment_min}" + query_ext + f" then 1 else 0 end as [inPaymentRange], \
            #           case when LEFT([ID], 1) IN ('B', 'S', 'U') then 'HCO' else 'HCP' end as [NodeType] \
            #           FROM [app2].[vAllNodes] \
            #           WHERE country = '{country}'" + org_ext
            # print("________query1________", query)
            # all_nodes = db.select_df(query)

            # Apply the query filters and transformations
            print("_______result1____")
            # result = df[(df['COUNTRY'] == data['country'])  & (df['PaymentAmount'] >= payment_min) & org_ext].copy() if org_ext else df[(df['COUNTRY'] == data['country']) & (df['PaymentAmount'] >= payment_min)].copy()
            # Convert df['ID'] to string
            df['PaymentAmount'] = df['PaymentAmount'].astype(int)
            result = df[
                (df['country'] == data['country']) &
                (df['PaymentAmount'] >= int(payment_min)) &
                (org_ext if isinstance(org_ext, pd.Series) else True)
                ].copy() if org != 'null' else df[
                (df['country'] == data['country']) &
                (df['PaymentAmount'] >= int(payment_min))
                ].copy()
            # if payment_min is not None:
            #     result = result[result['PaymentAmount'] >= payment_min]
            print("_______result0____", result)
            result['PaymentAmount'] = result['PaymentAmount'].astype(int)

            if payment_max is not None:
                result = result[result['PaymentAmount'] <= int(payment_max)]

            print("_______result____", result)
            result['node_id'] = result['ID']
            result['VendorName'] = result['Name']
            result['inPaymentRange'] = result['PaymentAmount'].apply(lambda x: 1 if x >= 1 else 0)
            result['NodeType'] = result['ID'].apply(lambda x: 'HCO' if x[0] in ['B', 'S', 'U'] else 'HCP')

            print("_______result2____", result.columns)
            # Select the required columns
            all_nodes = result[
                ['node_id', 'VendorName', 'PaymentAmount', 'InteractionCount', 'inPaymentRange', 'NodeType']]

            # Display the result
            print("all_nodes_______", all_nodes)

            # set base nodes as nodes connected to GSK
            # base_nodes = all_nodes[(all_nodes['PaymentAmount'] > 0) | (all_nodes['InteractionCount'] > 0)]
            # too many HCPs with interaction but no payments
            base_nodes = all_nodes[(all_nodes['PaymentAmount'] > 0)]

            payment_max = base_nodes['PaymentAmount'].max() if payment_max is None else payment_max

            # get edges between nodes in selected country
            # if org is hcp or hco only, collapse edge pairs where there is a middle node in common
            subqueryA = f"select hco_id, hcp_id from [app2].{edgeSource} where country = '{country}'"

            file_path = 'data/app2.vAllEdges.csv' if conn == 'weak' else 'data/app2.vStrongEdges.csv'
            print('filepath',file_path)
            df2 = pd.read_csv(file_path)
            df2['country'] = df2['country'].str.lower()


            if org == 'hco':
                # Filter for Country records
                df2 = df2[df2['country'] == data['country']]
                print(df2)
                # Perform self join on hcp_id and apply the condition hco_id_A < hco_id_B
                result2 = df2.merge(df2, on='hcp_id', suffixes=('_A', '_B'))
                print(result2)
                print(result2[['hco_id_A','hco_id_B']])
                result2 = result2[result2['hco_id_A'] != result2['hco_id_B']]

                # Select and rename the required columns
                result2 = result2[['hco_id_A', 'hco_id_B']].rename(columns={'hco_id_A': 'from_id', 'hco_id_B': 'to_id'})

                print("result576236_____", result2)

                # allow multiple edges between two HCO to enable increased thickness for multiple connections
                query = f'with t1 as ({subqueryA}) \
                        select A.[hco_id] as [from_id], B.[hco_id] as [to_id] from t1 A \
                        inner join t1 B on A.hcp_id = B.hcp_id where A.hco_id < B.hco_id'
            elif org == 'hcp':
                # Filter for Country records
                df2 = df2[df2['country'] == data['country']]

                # Perform the inner join on the hcp_id and apply the condition A.hco_id < B.hco_id
                result2 = df2.merge(df2, on='hco_id')
                print("result2", result2)
                result2 = result2[result2['hcp_id_x'] != result2['hcp_id_y']]

                # Select the required columns and rename them
                result2 = result2[['hcp_id_x', 'hcp_id_y']]
                result2.columns = ['from_id', 'to_id']
                query = f'with t1 as ({subqueryA}) \
                        select distinct A.[hcp_id] as [from_id], B.[hcp_id] as [to_id] from t1 A \
                        inner join t1 B on A.hco_id = B.hco_id where A.hcp_id < B.hcp_id'
            else:
                # Creating the temporary table t1
                t1 = df2[df2['country'] == data['country']][['hcp_id', 'hco_id']]

                # [['hcp_id', 'hco_id']]

                # Performing the self-join and filtering as per the query
                result2 = pd.merge(t1, t1, on='hcp_id')
                result2 = result2[result2['hco_id_x'] != result2['hco_id_y']]
                result2 = result2[['hcp_id', 'hco_id_x', 'hco_id_y']].drop_duplicates().rename(
                    columns={'hcp_id': 'from_id', 'hco_id_x': 'to_id'})
                print("t1_____", result2)
                # Renaming columns to match the query output
                # result2.columns = ['from_id', 'to_id']

                # only include edges where the hcp can join two different hcos
                query = f'with t1 as ({subqueryA}) \
                        select distinct A.[hcp_id] as [from_id], A.[hco_id] as [to_id] from t1 A \
                        inner join t1 B on A.hcp_id = B.hcp_id where A.hco_id <> B.hco_id'
            # print("________query2________", query)
            # orgType_edges = db.select_df(query)
            orgType_edges = result2
            print("______________orgType_edges______________", orgType_edges)
            base_nodes['node_id'] = base_nodes['node_id'].astype(str)
            orgType_edges['from_id'] = orgType_edges['from_id'].astype(str)
            orgType_edges['to_id'] = orgType_edges['from_id'].astype(str)

            # join to the base nodes
            orgType_edges = orgType_edges.merge(
                base_nodes[['node_id', 'inPaymentRange']], how='left', left_on=['from_id'], right_on=['node_id']
            )
            orgType_edges = orgType_edges.merge(
                base_nodes[['node_id', 'inPaymentRange']], how='left', left_on=['to_id'], right_on=['node_id'],
                suffixes=['_from', '_to']
            )

            # filter to include only edges either to or from one of the base nodes
            orgType_edges = orgType_edges[
                ~(pd.isna(orgType_edges['node_id_from'])) | ~(pd.isna(orgType_edges['node_id_to']))]

            # look for additional nodes that join to an in-scope edge but are not in the base nodes
            additional_nodes = all_nodes.merge(
                orgType_edges[pd.isna(orgType_edges['node_id_from'])][['from_id']].drop_duplicates(), how='left',
                left_on=['node_id'], right_on=['from_id']
            )
            additional_nodes = additional_nodes.merge(
                orgType_edges[pd.isna(orgType_edges['node_id_to'])][['to_id']].drop_duplicates(), how='left',
                left_on=['node_id'], right_on=['to_id']
            )
            additional_nodes = additional_nodes[
                ~(pd.isna(additional_nodes['from_id'])) | ~(pd.isna(additional_nodes['to_id']))]

            # node has page, shape, color, id, label, title, designation
            # edge has from, to, page
            nodes_list = []
            edges_list = []

            x = {
                "id": str(10001),
                "x": 0,
                "y": 0,
                "fixed": {
                    "x": True,
                    "y": True
                },
                "label": "GSK",
                "title": "GSK",
                "color": "#fdfd00",
                "size": 100,
                "shape": "image",
                "image": "https://www.mxp-webshop.de/media/image/20/82/24/GSK-Logo.png"
            }
            nodes_list.append(x)

            for rowIndex, row in base_nodes.iterrows():
                node_dict = dict()
                node_dict['page'] = 1
                node_dict['id'] = row['node_id']
                node_dict['shape'] = 'dot'
                if row['NodeType'] == 'HCO':
                    node_dict['color'] = "#fb7e81"
                else:
                    node_dict['color'] = "#95c0f9"
                node_dict['label'] = row['VendorName']
                node_dict['title'] = row['VendorName']
                node_dict['designation'] = ""
                node_dict['size'] = 50 if row['inPaymentRange'] == 1 else 10
                nodes_list.append(node_dict)

            for rowIndex, row in additional_nodes.iterrows():
                node_dict = dict()
                node_dict['page'] = 1
                node_dict['id'] = row['node_id']
                node_dict['shape'] = 'dot'
                if row['NodeType'] == 'HCO':
                    node_dict['color'] = "#fb7e81"
                else:
                    node_dict['color'] = "#95c0f9"
                node_dict['label'] = row['VendorName']
                node_dict['title'] = row['VendorName']
                node_dict['designation'] = ""
                node_dict['size'] = 50 if row['inPaymentRange'] == 1 else 10
                nodes_list.append(node_dict)

            print("orgType_edges______________________", orgType_edges)
            for rowIndex, row in orgType_edges.iterrows():
                edge_dict = dict()
                edge_dict['from'] = row['from_id']
                edge_dict['to'] = row['to_id']
                edge_dict['page'] = 1
                # edge_dict['width'] = row['inPaymentRange_from'] + row['inPaymentRange_to'] + 1
                edges_list.append(edge_dict)

            final_result = dict()
            graph = dict()

            graph['nodes'] = nodes_list
            graph['edges'] = edges_list
            graph['price_range'] = [1, int(payment_max)]
            final_result['counter'] = 5
            final_result['graph'] = graph
            final_result['events'] = {}

            if link.lower() == 'negative':
                neg_nodes_list = []
                neg_nodes_list.append(x)
                with open("./data/outputhco.json", encoding='latin-1') as inputfile:
                    hco_news = json.load(inputfile)
                with open("./data/NewhcpNewsHeadlines.json", encoding='latin-1') as inputfile:
                    hcp_news = json.load(inputfile)
                for i in nodes_list:
                    isNegative = self.get_negative_news(i["title"], hco_news, hcp_news, i["color"])
                    if isNegative:
                        neg_nodes_list.append(i)
                graph['nodes'] = neg_nodes_list
            return True, "graph_by_country", final_result
        except Exception as e:
            print("Deepdive.graph_by_country(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"

    def data_by_country(self, data):  # tbd
        try:
            # YOUR CODE HERE
            # REMOVE THIS ONCE IMPLEMENTED
            # if data["country"].strip().lower() == "brazil":
            #     _ret = self.read_json("brazil.json")
            # if data["country"].strip().lower() == "spain":
            #     _ret = self.read_json("spain.json")
            # if data["country"].strip().lower() == "usa":
            #     _ret = self.read_json("usa.json")
            # TILL HERE
            print("data____________",data)
            country = data['country']
            conn = data.get('connection')
            link = data.get('link')
            if country == 'null':
                True, "data_by_country", []

            currency_mapping = {'usa': 'USD', 'brazil': 'BRL', 'spain': 'EUR'}
            currency = currency_mapping.get(country.lower())
            file_path = 'data/app2.vPayments.csv'
            df = pd.read_csv(file_path)
            print("currency_mapping.get(country)___", type(currency_mapping.get(country)), type(df['Currency']))
            filtered_df = df[df['Currency'] == currency_mapping.get(country)]
            payments = filtered_df[['VendorName', 'InvoiceLineAmountLocal', 'Currency']]
            print("payments___________", payments.head(3))
            print("payments.shape[0]___", payments.shape[0])
            payments = payments.copy()  # Make a copy of the DataFrame
            payments.loc[:, 'Quartile'] = pd.qcut(payments['InvoiceLineAmountLocal'], q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'])
            payments = payments.copy()  # Make a copy of the DataFrame
            payments['Quartile'] = pd.qcut(payments['InvoiceLineAmountLocal'], q=4, labels=['Q1', 'Q2', 'Q3', 'Q4'])
            print("payments_hgygg__________", payments)
            if (data['min'] == '0' and data['max'] == '0') or (data['min'] == 'null' and data['max'] == 'null'):
                if country == 'usa':
                    min = 10000
                else:
                    min = 5000
                max = payments['InvoiceLineAmountLocal'].max()

            else:
                min = int(data['min'])
                max = int(data['max'])

            # payments = payments[payments['InvoiceLineAmountLocal']>=10000]

            payments = payments[(payments['InvoiceLineAmountLocal'] >= min) & (payments['InvoiceLineAmountLocal'] <= max)]
            print(payments.shape[0])
            payments['VendorName'] = payments['VendorName'].str.lower()
            payments = payments[['VendorName']].drop_duplicates()

            edges_list = []

            print("payments", payments)
            if conn == 'weak':
                file_path1 = 'data/app2.vAllEdges.csv'
                df = pd.read_csv(file_path1)
                df_country = df[df['country'] == country.upper()]
                count_hco_df = df_country.groupby('hcp_id').agg(
                    count_hco=('hco_id', 'nunique')
                ).reset_index()
                count_hco_df = count_hco_df[count_hco_df['count_hco'] >= 1]
                merged_df = pd.merge(df_country, count_hco_df, on='hcp_id')
                final_df = merged_df.groupby(['hco_id', 'hcp_id']).size().reset_index(name='count')
                final_df = final_df[final_df['count'] == 1]
                final_df.rename(columns={'hco_id': 'from', 'hcp_id': 'to'}, inplace=True)
            else:
                file_path1 = 'data/app2.vStrongEdges.csv'
                df = pd.read_csv(file_path1)
                df_country = df[df['country'] == country.upper()]
                count_hco_df = df_country.groupby('hcp_id').agg(
                    count_hco=('hco_id', 'nunique')
                ).reset_index()
                count_hco_df = count_hco_df[count_hco_df['count_hco'] >= 1]
                merged_df = pd.merge(df_country, count_hco_df, on='hcp_id')
                final_df = merged_df.groupby(['hco_id', 'hcp_id']).size().reset_index(name='count')
                final_df = final_df[final_df['count'] == 1]
                final_df.rename(columns={'hco_id': 'from', 'hcp_id': 'to'}, inplace=True)

            hcp_edges = final_df
            print("hcp_edges.shape[0]", hcp_edges.shape[0])
            file_path2 = 'data/app2.vHCP.csv'
            df = pd.read_csv(file_path2)

            filtered_df = df[df['country'] == country.upper()]
            print("filtered_df__vHCP__________", filtered_df)
            hcp_names = filtered_df[['id', 'hcp_name']].drop_duplicates()
            hcp_names.rename(columns={'id': 'hcp_id'}, inplace=True)

            file_path2 = 'data/app2.vHco.csv'
            df = pd.read_csv(file_path2)
            filtered_df = df[df['COUNTRY'] == country.upper()]

            hco_names = filtered_df[['ID', 'NAME']].drop_duplicates()
            hco_names.rename(columns={'ID': 'internal_hco_id'}, inplace=True)
            print("filtered_df__vHco__________", filtered_df.head())
            merged_hcp = hcp_edges.merge(hcp_names, how='left', left_on=['to'], right_on=['hcp_id'])
            merged_hcp['hcp_name'] = merged_hcp['hcp_name'].str.lower()
            print("merged_hcp", merged_hcp)
            merged_hcp_hco = merged_hcp.merge(hco_names, how='left', left_on=['from'], right_on=['internal_hco_id'])
            print("merged_hcp_hco", merged_hcp_hco)
            merged_hcp_hco['hco_name'] = merged_hcp_hco['NAME'].str.lower()

            print("merged_hcp_hco.shape[0]", merged_hcp_hco.shape[0])

            distinct_hcps = merged_hcp_hco[['to', 'hcp_name']].drop_duplicates()

            distinct_hcos = merged_hcp_hco[['from', 'hco_name']].drop_duplicates()
            print("distinct_hcps.shape[0]", distinct_hcps.shape[0])
            print("distinct_hcos.shape[0]", distinct_hcos.shape[0])

            merged_hcp_payments = distinct_hcps.merge(payments, how='inner', left_on=['hcp_name'],
                                                      right_on=['VendorName'])

            merged_hco_payments = distinct_hcos.merge(payments, how='inner', left_on=['hco_name'],
                                                      right_on=['VendorName'])

            print("merged_hcp_payments.shape[0]", merged_hcp_payments.shape[0])
            print("merged_hco_payments.shape[0]", merged_hco_payments.shape[0])

            # for those HCPs which do not ahve payments but are a part of the hcos and have multiple connections
            if conn == 'weak':
                file_path1 = 'data/app2.vAllEdges.csv'
                df = pd.read_csv(file_path1)
                df_country = df[df['country'] == country]
                count_hco_df = df_country.groupby('hcp_id').agg(
                    count_hco=('hco_id', 'nunique')
                ).reset_index()
                count_hco_df = count_hco_df[count_hco_df['count_hco'] > 1]
                merged_df = pd.merge(df_country, count_hco_df, on='hcp_id')
                final_df = merged_df.groupby(['hco_id', 'hcp_id']).size().reset_index(name='count')
                additional_hcps = final_df[final_df['count'] == 1]
                additional_hcps.rename(columns={'hco_id': 'from', 'hcp_id': 'to'}, inplace=True)

            else:
                query = f"select \
                                            a.hco_id as 'from', a.hcp_id as 'to', count(*) as 'count' \
                                            from [app2].[vStrongEdges] a \
                                            join \
                                            (select hcp_id, count(distinct hco_id) as count_hco from [app2].[vStrongEdges] where country = '{country}' group by hcp_id having count(distinct hco_id) > 1)b \
                                            on a.hcp_id = b.hcp_id \
                                            where a.country = '{country}' \
                                            group by a.hco_id, a.hcp_id \
                                            having count(*) =1"

                file_path1 = 'data/app2.vStrongEdges.csv'
                df_strong_edges = pd.read_csv(file_path1)
                # Filter data by country
                df_country = df_strong_edges[df_strong_edges['country'] == country.upper()]

                # Get the count of distinct hco_id per hcp_id
                count_hco_per_hcp = df_country.groupby('hcp_id')['hco_id'].nunique().reset_index()
                count_hco_per_hcp.columns = ['hcp_id', 'count_hco']

                # Filter to only include hcp_id with more than 1 distinct hco_id
                filtered_hcp = count_hco_per_hcp[count_hco_per_hcp['count_hco'] > 1]

                # Join to get the relevant rows
                df_filtered = df_country.merge(filtered_hcp, on='hcp_id')

                # Group by hco_id and hcp_id, and count occurrences
                grouped_df = df_filtered.groupby(['hco_id', 'hcp_id']).size().reset_index(name='count')

                # Filter groups where count is 1
                additional_hcps = grouped_df[grouped_df['count'] == 1]

                # Rename columns to match SQL output
                additional_hcps.rename(columns={'hco_id': 'from', 'hcp_id': 'to'}, inplace=True)

            print("additional_hcps", additional_hcps)
            rel_hcps = additional_hcps.merge(merged_hco_payments, how="inner", left_on=["from"], right_on=['from'])

            print(rel_hcps.shape[0])
            print(
                "---------------------------------------------------------------------------------------------------------------")
            print(rel_hcps)
            rel_hcps = rel_hcps[['from', 'to']]
            print(
                "*******************************************************************************************************************")
            print(rel_hcps)

            merged_hco_payments_edges = merged_hco_payments[['from', 'VendorName']].drop_duplicates()
            print("____merged_hco_payments_edges.shape[0]______",merged_hco_payments_edges.shape[0])

            merged_hcp_payments_edges = merged_hcp_payments[['to', 'VendorName']].drop_duplicates()
            print("merged_hcp_payments_edges_____",merged_hcp_payments_edges)

            merged_hcp_payments_edges = merged_hcp_payments_edges.rename(
                columns={'to': 'hcp_id_distinct_payments', 'VendorName': 'vn_distinct_payments'})

            hcp_edges = merged_hcp_hco.merge(merged_hcp_payments_edges, how='inner', left_on=['to'],
                                             right_on=['hcp_id_distinct_payments'])
            hcp_edges = hcp_edges[['from', 'to']]
            print(hcp_edges.shape[0])

            final_hcps = pd.concat([hcp_edges, rel_hcps], ignore_index=True).drop_duplicates()
            print("£££££££££££££££££££££££££££££££££££££££££££££££")
            print(final_hcps)
            print(final_hcps.shape[0])

            print("merged_hco_payments_edges", merged_hco_payments_edges)
            # Removing the connections back to GSK for now
            for i,row in merged_hco_payments_edges.iterrows():
                edges_dict = dict()

                edges_dict['to'] = row['from']
                edges_dict['from'] = '10001'
                edges_dict['page'] = 1
                edges_list.append(edges_dict)

            for i,row in hcp_edges.iterrows():
                edges_dict = dict()

                edges_dict['to'] = row['to']
                edges_dict['from'] = '10001'
                edges_dict['page'] = 1
                edges_list.append(edges_dict)

            for i, row in final_hcps.iterrows():
                hcp_dict = dict()
                hcp_dict["from"] = row['from']
                hcp_dict["to"] = row["to"]
                hcp_dict['page'] = 1

                edges_list.append(hcp_dict)
                print(len(edges_list))
            print(len(edges_list))

            node_list = []

            x = {
                "id": str(10001),
                "x": 0,
                "y": 0,
                "fixed": {
                    "x": True,
                    "y": True
                },
                "label": "GSK",
                "title": "GSK",
                "color": "#fdfd00",
                "size": 100,
                "shape": "image",
                "image": "https://www.mxp-webshop.de/media/image/20/82/24/GSK-Logo.png"
            }
            node_list.append(x)

            final_edges = pd.concat([merged_hco_payments_edges, final_hcps], ignore_index=True)
            print(final_edges)
            hcos_for_nodes = final_edges[['from']].drop_duplicates()
            hcps_for_nodes = final_edges[['to']].drop_duplicates()

            print(hcos_for_nodes.shape[0], ",", hcps_for_nodes.shape[0])

            if conn == 'weak':
                # SELECT a.id, a.hcp_name, a.country, max(b.designation) as 'designation' FROM [app2].[vHcp] a \
                #                         join [app2].[vAllEdges] b on a.Id = b.hcp_id where a.country = '{country}' group by a.Id, a.hcp_name, a.country
                file_path1 = 'data/app2.vAllEdges.csv'
                df_edges = pd.read_csv(file_path1)
                file_path2 = 'data/app2.vHCP.csv'
                df_hcp = pd.read_csv(file_path2)
                df_hcp_country = df_hcp[df_hcp['country'] == country.upper()]

                # Merge (join) the DataFrames on the id and hcp_id columns
                filtered_merged_df = pd.merge(df_hcp_country, df_edges, left_on='id', right_on='hcp_id')
                print("filtered_merged_df", filtered_merged_df)
                filtered_merged_df.rename(columns={'id_x': 'id', 'country_x': 'country'}, inplace=True)
                # Group by id, hcp_name, and country, and take the max of designation
                result_df = filtered_merged_df.groupby(['id', 'hcp_name', 'country'])['designation'].max().reset_index()

                df = result_df
            else:
                # Load the CSV files into DataFrames
                df_hcp = pd.read_csv('data/app2.vHCP.csv')
                df_strong_edges = pd.read_csv('data/app2.vStrongEdges.csv')

                # Filter df_hcp by country
                df_hcp_filtered = df_hcp[df_hcp['country'] == country.upper()]

                # Perform the join between df_hcp and df_strong_edges on the 'Id' column
                df_joined = pd.merge(df_hcp_filtered, df_strong_edges, left_on='id', right_on='hcp_id')
                print("df_joined", df_joined.head())
                # Group by the necessary columns and calculate the max of 'designation'
                df_joined.rename(columns={'id_x': 'id', 'country_x': 'country'}, inplace=True)
                result_df = df_joined.groupby(['id', 'hcp_name', 'country'])['designation'].max().reset_index()

                print("result_df", result_df)

                # file_path1 = 'data/app2.vStrongEdges.csv'
                # df_edges = pd.read_csv(file_path1)
                # file_path2 = 'data/app2.vHCP.csv'
                # df_hcp = pd.read_csv(file_path2)
                # df_hcp_country = df_hcp[df_hcp['country'] == country]
                # merged_df = pd.merge(df_hcp_country, df_edges, left_on='id', right_on='hcp_id', how='left')
                # result_df = merged_df.groupby(['id', 'hcp_name', 'country']).agg(
                #     designation=('designation', 'max')
                # ).reset_index()
                df = result_df
            nodes_df = df[['id', 'hcp_name', 'designation', 'country']]

            nodes_merged = hcps_for_nodes.merge(nodes_df, how='inner', left_on=["to"], right_on=["id"])
            print("nodes_df", nodes_df)
            nodes_merged = nodes_merged.drop_duplicates(subset=['id'])

            if conn == 'weak':
                file_path1 = 'data/app2.vAllEdges.csv'
                df_edges = pd.read_csv(file_path1)
                file_path2 = 'data/app2.vHco.csv'
                df_hco = pd.read_csv(file_path2)
                print("xyz")
                # Merge the DataFrames on ID and hco_id columns
                merged_df = pd.merge(df_hco, df_edges, left_on='ID', right_on='hco_id')

                # Filter by the specified country
                filtered_df = merged_df[merged_df['COUNTRY'] == country]

                # Group by COUNTRY, HCO, and ID, then take the max of hco_id
                result_df = filtered_df.groupby(['COUNTRY', 'NAME', 'ID'])['hco_id'].max().reset_index()

                query = f"select a.COUNTRY, a.HCO, a.ID, max(b.hco_id) from [app2].[vHco] a join [app2].[vAllEdges] b on a.ID = b.hco_id where a.COUNTRY = '{country}' group by a.COUNTRY, a.HCO, a.ID"
            else:
                # query = f"select a.COUNTRY, a.HCO, a.ID, max(b.hco_id) from [app2].[vHco] a join [app2].[vStrongEdges] b on a.ID = b.hco_id where a.COUNTRY = '{country}' group by a.COUNTRY, a.HCO, a.ID"

                # Load CSV files into pandas DataFrames
                df_hco = pd.read_csv('data/app2.vHco.csv')  # Replace with the path to your vHco CSV
                df_strong_edges = pd.read_csv('data/app2.vStrongEdges.csv')  # Replace with the path to your vStrongEdges CSV

                # Filter the data for the specific country
                df_hco_filtered = df_hco[df_hco['COUNTRY'] == country.upper()]

                # Perform the join operation (equivalent to SQL JOIN)
                merged_df = pd.merge(df_hco_filtered, df_strong_edges, left_on='ID', right_on='hco_id', how='inner')

                # Group by COUNTRY, HCO, and ID, and get the maximum hco_id
                result_df = merged_df.groupby(['COUNTRY', 'NAME', 'ID'])['hco_id'].max().reset_index()

            # Execute the query
            hco_df = result_df

            nodes_hco_df = hco_df.rename(columns={'NAME': 'label', 'ID': 'id', 'COUNTRY': 'country'})

            nodes_hco_merged = hcos_for_nodes.merge(nodes_hco_df, how='inner', left_on=["from"], right_on=["id"])

            # nodes_merged = nodes_merged.drop_duplicates(subset = ['Id'])
            # nodes_hco_merged = nodes_hco_merged.drop_duplicates(subset = ['id'])

            print(nodes_merged)
            print(nodes_hco_merged)
            a = 0
            for i, row in nodes_hco_merged.iterrows():
                a += 1
                node_hco_dict = dict()
                node_hco_dict['page'] = 1

                node_hco_dict['shape'] = 'dot'
                node_hco_dict['color'] = "#fb7e81"
                node_hco_dict['id'] = row['id']
                node_hco_dict['label'] = row['label']
                node_hco_dict['title'] = row['label']
                node_hco_dict['designation'] = ""
                node_list.append(node_hco_dict)
            print(f"number of hco nodes is {a}")
            b = 0
            for i, row in nodes_merged.iterrows():
                if row['to'] == 'NaN':
                    print("if in")
                    continue
                b += 1
                node_dict = dict()
                node_dict['shape'] = 'dot'
                node_dict['color'] = "#95c0f9"
                node_dict['id'] = row['id']
                node_dict['label'] = row['hcp_name']
                node_dict['title'] = row['hcp_name']
                node_dict['designation'] = row['designation']
                node_dict['page'] = 1

                node_list.append(node_dict)
            print(f"number of hcp nodes is {b}")

            final_result = dict()
            graph = dict()

            final_node_list = []
            final_edge_list = []
            for item in node_list:
                final_node_list.append(item)

            for item in edges_list:
                final_edge_list.append(item)

            graph['nodes'] = final_node_list
            graph['edges'] = final_edge_list
            graph['price_range'] = [min, int(max)]
            final_result['counter'] = 5
            final_result['graph'] = graph
            final_result['events'] = {}

            _ret = final_result

            if link.lower() == 'negative':
                print("negative = data.get('negative')")
                neg_nodes_list = []
                neg_nodes_list.append(x)
                with open("./data/outputhco.json", encoding='latin-1') as inputfile:
                    hco_news = json.load(inputfile)
                with open("./data/NewhcpNewsHeadlines.json", encoding='latin-1') as inputfile:
                    hcp_news = json.load(inputfile)
                for i in final_node_list:
                    #print("isNegative____________________________", i["title"])
                    isNegative = self.get_negative_news(i["title"], hco_news, hcp_news, i["color"])
                    #print("isNegative____________________________", isNegative)
                    if isNegative:
                        neg_nodes_list.append(i)
                graph['nodes'] = neg_nodes_list

            return True, "data_by_country", _ret
        except Exception as e:
            print("Deepdive.data_by_country(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"

    def data_by_node(self, data):  # tbd
        try:
            # _ret = self.read_json("subGraph.json")
            # return True, "data_by_node", _ret
            iden = data['id']
            conn = data.get('connection')
            print(iden, conn)
            # query nodes and edges
            edgeSource = 'vAllEdges' if conn == 'weak' else 'vStrongEdges'
            data_df = pd.read_csv(f'data/app2.{edgeSource}.csv')

            extra_edges_final = pd.DataFrame(columns=['hcp_id', 'hco_id'])
            if iden == 'null':
                True, "null_data", []

            if 'B' in iden or 'S' in iden or 'U' in iden:
                filtered_df = data_df[data_df['hco_id'] == iden]
            else:
                iden = str(iden)
                data_df['hcp_id'] = data_df['hcp_id'].astype(str)
                filtered_df = data_df[data_df['hcp_id'] == iden]
                print("filtered",filtered_df)
            result_df = (filtered_df
                         .groupby(['hco_id', 'hcp_id'])
                         .size()
                         .reset_index(name='count')
                         .assign(gsk=10001))

            # Rename columns to match the SQL output
            result_df = result_df.rename(columns={'hco_id': 'hco', 'hcp_id': 'hcp'})
            edges = result_df
            print(edges)
            print(edges.shape[0])
            edges_list = []
            if 'B' in iden or 'S' in iden or 'U' in iden:
                char_first = iden[0]
                hcps = edges[['hcp']]
                filtered_df = data_df[
                    (data_df['hco_id'] != iden) &
                    (data_df['hco_id'].str.startswith(char_first))
                    ]
                result_df = (filtered_df
                             .groupby(['hcp_id', 'hco_id'])
                             .size()
                             .reset_index(name='count'))

                extra_edges = result_df
                if not extra_edges.empty:
                    print("inif")
                    extra_edges_final = hcps.merge(extra_edges, how='inner', left_on=['hcp'], right_on=['hcp_id'])
                    extra_edges_final = extra_edges_final[['hco_id', 'hcp_id']].drop_duplicates()
                    extra_edges_final = extra_edges_final.dropna(subset=['hcp_id'])
                    print(extra_edges_final.shape[0])

                    for i, row in extra_edges_final.iterrows():
                        edges_dict = dict()
                        print(row['hcp_id'])
                        if row['hcp_id']:
                            edges_dict['from'] = row['hcp_id']
                            edges_dict['to'] = row['hco_id']
                            edges_list.append(edges_dict)

            for i, row in edges.iterrows():
                edges_dict = dict()
                edges_dict['from'] = row['gsk']
                edges_dict['to'] = row['hco']
                edges_list.append(edges_dict)

            for i, row in edges.iterrows():
                edges_dict = dict()
                edges_dict['from'] = row['hco']
                edges_dict['to'] = row['hcp']
                edges_list.append(edges_dict)

            print(edges_list)

            node_list = []

            x = {
                "id": str(10001),
                "x": 0,
                "y": 0,
                "fixed": {
                    "x": True,
                    "y": True
                },
                "label": "GSK",
                "title": "GSK",
                "color": "#fdfd00",
                "size": 75,
                "shape": "image",
                "image": "https://www.mxp-webshop.de/media/image/20/82/24/GSK-Logo.png"
            }
            node_list.append(x)

            if not extra_edges_final.empty:
                hcp1 = extra_edges_final[['hcp_id']].rename(columns={'hcp_id': 'hcp'})
                hcp2 = edges[['hcp']]
                hcps = pd.concat([hcp1, hcp2], ignore_index=True)
            else:
                hcps = edges[['hcp']]

            hcps = hcps.drop_duplicates()

            if not extra_edges_final.empty:
                hco1 = extra_edges_final[['hco_id']].rename(columns={'hco_id': 'hco'})
                hco2 = edges[['hco']]
                hcos = pd.concat([hco1, hco2], ignore_index=True)
            else:
                hcos = edges[['hco']]

            hcos = hcos.drop_duplicates()
            vHcp_df = pd.read_csv('data/app2.vHCP.csv', encoding='ISO-8859-1')
            vHcp_df.rename(columns={'ï»¿id': 'id'}, inplace=True)
            vHcp_df['id'] = vHcp_df['id'].astype(str)
            data_df['hcp_id'] = data_df['hcp_id'].astype(str)
            merged_df = pd.merge(vHcp_df, data_df, left_on='id', right_on='hcp_id')
            print(merged_df.columns)
            print(merged_df)
            result_df = (merged_df
                         .groupby(['hcp_id', 'hcp_name', 'country_x'])
                         .agg(designation=('designation', 'max'))
                         .reset_index()
                         .rename(columns={'hcp_id': 'id', 'country_x': 'country'}))
            df = result_df
            nodes_df = df[['id', 'hcp_name', 'designation', 'country']]
            print(nodes_df)
            hcps['hcp'] = hcps['hcp'].astype(str)
            nodes_df['id'] = nodes_df['id'].astype(str)
            nodes_merged = hcps.merge(nodes_df, how='inner', left_on=["hcp"], right_on=["id"])
            print(nodes_merged)
            nodes_merged = nodes_merged.drop_duplicates(subset=['id'])

            vHco_df = pd.read_csv('data/app2.vHco.csv')
            merged_df = pd.merge(vHco_df, data_df, left_on='ID', right_on='hco_id')
            print(merged_df.columns)
            result_df = (merged_df
                         .groupby(['COUNTRY', 'NAME', 'ID'])
                         .agg(max_hco_id=('hco_id', 'max'))
                         .reset_index())
            # Execute the query
            hco_df = result_df

            nodes_hco_df = hco_df.rename(columns={'NAME': 'label', 'ID': 'id', 'COUNTRY': 'country'})
            nodes_hco_merged = hcos.merge(nodes_hco_df, how='inner', left_on=["hco"], right_on=["id"])
            print(nodes_hco_merged.columns)
            a = 0
            for i, row in nodes_hco_merged.iterrows():
                a += 1
                node_hco_dict = dict()
                node_hco_dict['page'] = 1

                node_hco_dict['shape'] = 'dot'
                node_hco_dict['color'] = "#fb7e81"
                node_hco_dict['id'] = row['id']
                node_hco_dict['label'] = row['label']
                node_hco_dict['title'] = row['label']
                node_hco_dict['designation'] = ""
                node_list.append(node_hco_dict)
            print(f"number of hco nodes is {a}")
            b = 0
            for i, row in nodes_merged.iterrows():
                b += 1
                node_dict = dict()
                node_dict['shape'] = 'dot'
                node_dict['color'] = "#95c0f9"
                node_dict['id'] = row['id']
                node_dict['label'] = row['hcp_name']
                node_dict['title'] = row['hcp_name']
                node_dict['designation'] = row['designation']
                node_dict['page'] = 1

                node_list.append(node_dict)
            print(f"number of hcp nodes is {b}")

            final_result = dict()
            graph = dict()

            graph['nodes'] = node_list
            graph['edges'] = edges_list
            graph['pagination'] = {'first_page': 1, 'last_page': 1}
            final_result['counter'] = 5
            final_result['graph'] = graph
            final_result['events'] = {}

            _ret = final_result

            return True, "data_by_node", _ret
        except Exception as e:
            print("Deepdive.data_by_node(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"

    def timeline(self, data):  # tbd
        try:
            print(type(datetime))
            # YOUR CODE HERE
            # REMOVE THIS ONCE IMPLEMENTED
            # _ret = self.read_json("chronology.json")
            # return True, "timeline", _ret
            timeline_list = []
            iden = data['id']
            if iden == 'null':
                True, "null_data", []

            if 'B' in data['id'] or 'S' in data['id'] or 'U' in data['id']:
                df = pd.read_csv('data/app2.vHco.csv')
                df_filtered = df[df['ID'] == iden]
                entity = df_filtered[['NAME', 'payment_hco_id']]
                for i, row in entity.iterrows():
                    entity_name = row['NAME']
                    payment_id = row['payment_hco_id']
                    break
                with open("./data/outputhco.json", encoding='latin-1') as inputfile:
                    hco_news = json.load(inputfile)

                for article in hco_news:
                    print(article['hco'])
                    if article['hco'].encode('ISO-8859-1').decode('utf-8') == entity_name:
                        event_dict = dict()
                        event_dict["id"] = data['id']
                        event_dict["tag"] = article['hco'] + ' : ' + ' External News Event'
                        event_dict["category"] = 'External News Event'
                        event_dict['date'] = article['date']
                        event_dict['sortdate'] = datetime.date.fromtimestamp(
                            datetime.datetime.strptime(article['date'], "%Y-%m-%d").timestamp())
                        event_dict['description'] = article['link']
                        timeline_list.append(event_dict)
            else:
                df = pd.read_csv('data/app2.vHCP.csv')
                df['id'] = df['id'].astype(str)
                df_filtered = df[df['id'] == iden]
                print("found id",df_filtered)
                entity = df_filtered[['hcp_name', 'payment_hcp_id']]
                for i, row in entity.iterrows():
                    entity_name = row['hcp_name']
                    payment_id = row['payment_hcp_id']
                    break
                with open("./data/NewhcpNewsHeadlines.json", encoding='latin-1') as inputfile:
                    hco_news = json.load(inputfile)

                for article in hco_news:
                    if article['hcp'].encode('ISO-8859-1').decode('utf-8') == entity_name:
                        event_dict = dict()
                        event_dict["id"] = data['id']
                        event_dict["tag"] = article['hco'] + ' : ' + article['hcp'] + ' : ' + ' External News Event'
                        event_dict["category"] = 'External News Event'
                        event_dict['date'] = article['date']
                        event_dict['sortdate'] = datetime.date.fromtimestamp(
                            datetime.datetime.strptime(article['date'], "%Y-%m-%d").timestamp())
                        event_dict['description'] = article['link']
                        timeline_list.append(event_dict)

            print(entity_name, payment_id, timeline_list)
            import time
            time.sleep(1)  # testing reduced time here; was 5
            # interactions:
            file_path = 'data/app2.vInteractions.csv'
            df = pd.read_csv(file_path, encoding='ISO-8859-1')
            print(df)
            df_filtered = df[df['ID'] == iden]
            print('afterfiltering', df_filtered)
            df_filtered = df_filtered[
                ['InteractionType', 'InteractionSubtype', 'InteractionTopic', 'ParentCallId', 'InteractionStart',
                 'HcpName']]
            interactions = df_filtered.sort_values(by='InteractionStart')
            if not interactions.empty:
                for i, row in interactions.iterrows():
                    event_dict = dict()
                    event_dict["id"] = data['id']
                    event_dict["tag"] = row['InteractionSubtype'] + ' with ' + row["HcpName"]
                    event_dict["category"] = row['InteractionType']
                    event_dict['date'] = str(row['InteractionStart'])
                    event_dict['sortdate'] = row['InteractionStart']
                    event_dict['description'] = row['ParentCallId'] + ' | ' + row['InteractionTopic']
                    timeline_list.append(event_dict)

            # payments
            df = pd.read_csv('data/app2.vPayments.csv')
            df_filtered = df[df['VendorNumber'] == payment_id]
            df_filtered = df_filtered[
                ['ThirdPartyPaymentsLineId', 'InvoiceGIDate', 'PaymentType', 'PaymentSubtype', 'InvoiceLineAmountLocal',
                 'AllText', 'Currency', 'VendorNumber', 'VendorName']]
            payments = df_filtered.sort_values(by='InvoiceGIDate')
            print("error", payments.columns)
            if not payments.empty:
                for i, row in payments.iterrows():
                    event_dict = dict()
                    event_dict["id"] = data['id']
                    event_dict["tag"] = str(row['PaymentSubtype']) + ' for ' + str(row["VendorName"])
                    event_dict["category"] = row['PaymentType']
                    event_dict['date'] = str(row['InvoiceGIDate'])
                    event_dict['sortdate'] = row['InvoiceGIDate']
                    x = '{:,.2f}'.format(row['InvoiceLineAmountLocal'])
                    print("type",type(x))
                    x=str(x)
                    event_dict['description'] = x + ' ' + str(row['Currency']) + ' | ' + str(row['AllText'])  # format with thousands separaters and 2dp
                    timeline_list.append(event_dict)

            # def convert_to_date(date_str):
            #     # Define the date formats you expect
            #     formats = ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y")  # Add other formats if needed
            #
            #     for fmt in formats:
            #         try:
            #             return datetime.strptime(date_str, fmt).date()
            #         except ValueError:
            #             continue
            #
            #     raise ValueError(f"Unrecognized date format: {date_str}")
            #
            # for item in timeline_list:
            #     item['sortdate'] = convert_to_date(item['sortdate'])
            #
            # newlist = sorted(timeline_list, key=lambda d: d['sortdate'])
            # _ret = newlist
            _ret = timeline_list
            print(_ret)
            return True, "timeline", _ret

        except Exception as e:
            print("Deepdive.timeline(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"

    def ext_events(self, data):  # tbd
        try:
            # YOUR CODE HERE
            # REMOVE THIS ONCE IMPLEMENTED
            # _ret = self.read_json("eventTimeline.json")
            iden = data['id']
            if iden == 'null':
                True, "null_data", []

            timeline_list = []
            if 'B' in data['id'] or 'S' in data['id'] or 'U' in data['id']:
                # query = (f"select HCO from [app2].[vHco] where I"
                #          f"D = '{iden}'")
                # db = MSSQLConnection()
                # entity = db.select_df(query)
                file_path = 'data/app2.vHco.csv'
                df = pd.read_csv(file_path)

                print("df_", df)
                # Convert df['ID'] to string
                df['ID'] = df['ID'].astype(str)
                entity = df[df['ID'] == iden].rename(columns={'NAME': 'HCO'})
                print("entity____", entity)

                entity_name = ""
                for i, row in entity.iterrows():
                    entity_name = row['HCO']
                    break
                with open("./data/outputhco.json", encoding='latin-1') as inputfile:
                    hco_news = json.load(inputfile)
                print("entity_name", entity_name)
                for article in hco_news:
                    if article['hco'].encode('ISO-8859-1').decode('utf-8') == entity_name:
                        event_dict = dict()
                        event_dict["id"] = data['id']
                        event_dict["title"] = article['title']
                        event_dict["hco"] = article['hco']
                        event_dict['source'] = article['source']
                        event_dict['date'] = article['date']
                        event_dict['link'] = article['link']
                        event_dict['country'] = article['country']
                        event_dict['collaborators'] = ''
                        event_dict['category'] = article['category']
                        event_dict['sentiment'] = article['sentiment']
                        event_dict['flag'] = 'HCO'
                        timeline_list.append(event_dict)
            else:
                query = f"select hcp_name from [app2].[vHcp] where Id = '{iden}'"
                # db = MSSQLConnection()
                # # entity = db.select_df(query)
                print("hcp news")
                file_path = 'data/app2.vHCP.csv'
                df = pd.read_csv(file_path, encoding='latin1')
                # pd.read_csv(file_path, encoding='latin1')
                print("df____", df)
                df.rename(columns={'ï»¿id': 'id'}, inplace=True)
                print(df.columns)
                iden = str(iden)
                df['id'] = df['id'].astype(str)
                entity = df[df['id'] == iden]

                print("entity____", entity)
                entity_name = ''
                for i, row in entity.iterrows():
                    entity_name = row['hcp_name']
                    break
                with open("./data/NewhcpNewsHeadlines.json", encoding='latin-1') as inputfile:
                    hco_news = json.load(inputfile)

                for article in hco_news:
                    if article['hcp'].encode('ISO-8859-1').decode('utf-8') == entity_name:
                        event_dict = dict()
                        event_dict["id"] = data['id']
                        event_dict["title"] = article['title']
                        event_dict["hco"] = article['hco']
                        event_dict["hcp"] = article['hcp']
                        event_dict['source'] = article['source']
                        event_dict['date'] = article['date']
                        event_dict['link'] = article['link']
                        event_dict['country'] = article['country']
                        event_dict['collaborators'] = ''
                        event_dict['category'] = article['category']
                        event_dict['sentiment'] = article['sentiment']
                        event_dict['flag'] = "HCP"

                        timeline_list.append(event_dict)
            _ret = timeline_list

            return True, "ext_events", _ret
        except Exception as e:
            print("Deepdive.ext_events(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"

    def overview(self, data):  # tbd
        try:
            # YOUR CODE HERE
            # REMOVE THIS ONCE IMPLEMENTED
            return_dict = dict()
            iden = data['id']
            if iden == 'null':
                True, "null_data", []

            currency_mapping = {'usa': 'USD', 'brazil': 'BRL', 'spain': 'EUR'}

            query = f"select ID, [Name], PaymentAmount, InteractionCount, LOWER(Country) as Country from [app2].[vAllNodes] where ID = '{iden}'"
            db = MSSQLConnection()
            # entity = db.select_df(query)
            file_path = 'data/app2.AllNodes.csv'
            df = pd.read_csv(file_path)
            print(type(iden))
            df['ID'] = df['ID'].astype(str)
            entity = df[df['ID'] == iden][['ID', 'Name', 'PaymentAmount', 'InteractionCount', 'country']].rename(
                columns={'country': 'Country'})
            # .rename(columns={'COUNTRY': 'Country'}))

            print("entity", entity)
            if not entity.empty:
                for i, row in entity.iterrows():
                    payment_amount = row['PaymentAmount']
                    total_interactions = row['InteractionCount']
                    entity_name = row['Name']
                    currency = currency_mapping[row['Country'].lower()]
                    break

            return_dict['totalPaymentMade'] = '{:,.2f}'.format(payment_amount) + ' ' + currency  # format with thousands separaters and 2dp

            return_dict['totalInteraction'] = str(total_interactions)
            return_dict['selectedName'] = entity_name
            print(return_dict, '00000000000000000000000000000000000000000000000000000000000000')
            if 'B' in data['id'] or 'S' in data['id'] or 'U' in data['id']:
                with open("./data/outputhco.json", encoding='latin-1') as inputfile:
                    hco_news = json.load(inputfile)
                i = 0
                for article in hco_news:
                    if article['hco'] == entity_name:
                        i += 1
                print(i, "total media articles")
            else:
                with open("./data/NewhcpNewsHeadlines.json", encoding='latin-1') as inputfile:
                    hco_news = json.load(inputfile)
                i = 0
                for article in hco_news:
                    if article['hcp'] == entity_name:
                        i += 1
            return_dict['mediaArticles'] = str(i)
            return_dict['riskIdentified'] = str(0)
            _ret = [return_dict]

            print(_ret)
            # _ret = self.read_json("overview.json")
            return True, "overview", _ret
        except Exception as e:
            print("Deepdive.overview(): " + str(e))
            traceback.print_exc()
            return False, "Something went wrong"
