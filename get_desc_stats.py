import os
import pandas as pd
import pickle as pkl
import time

def read_sts(path, _mask):
    li = []
    for file in path:
        if file.endswith(_mask):
            frame = pd.read_csv(file, index_col = 0, header = None, skiprows = 1)
            li.append(frame)
    df = pd.concat(li, axis = 0, ignore_index = True, sort = False)
    return df

def write_sts(df, outfile, topic):
    cols = ['Название ресурса', 'URL-адрес ресурса', 'Тематики ресурса',
           'Тип ресурса', 'Медиахолдинг', 'Данные Метрики',
           'Посетители (кросс-девайс)', 'Посетители (браузер)', 'Среднее время',
           'Доля пользователей приложения', 'Дневная аудитория', 'Период']
    df.columns = cols
    
    df['Процент в категории (браузер)'] = (df['Посетители (браузер)'])/(df['Посетители (браузер)']).sum()*100
    df['Процент в категории (кросс-девайс)'] = (df['Посетители (кросс-девайс)'])/(df['Посетители (кросс-девайс)']).sum()*100
    df['Процент в категории (браузер от кросс-девайс)'] = (df['Посетители (браузер)'])/(df['Посетители (кросс-девайс)']).sum()*100
    
    df['Динамика роста (браузер)'] = None
    df['Динамика роста (кросс-девайс)'] = None    
    df['Динамика роста (браузер от кросс-девайc)'] = None
    
    def get_growth(fd, url_list):
        new_df = fd.copy()
        new_cols = {'Посетители (кросс-девайс)': 6, 'Посетители (браузер)':7}
        for url in url_list:
            tmp_df = new_df[new_df['URL-адрес ресурса'] == url]
            for ind in range(len(tmp_df.index)):
                if ind != 0:
                    new_df['Динамика роста (браузер)'].ix[tmp_df.index[ind]] = (tmp_df.ix[tmp_df.index[ind], new_cols.get('Посетители (браузер)')] \
                                                                 - tmp_df.ix[tmp_df.index[ind - 1], new_cols.get('Посетители (браузер)')]) \
                    /tmp_df.ix[tmp_df.index[ind - 1], new_cols.get('Посетители (браузер)')]*100
                    new_df['Динамика роста (кросс-девайс)'].ix[tmp_df.index[ind]] = (tmp_df.ix[tmp_df.index[ind], new_cols.get('Посетители (кросс-девайс)')] \
                                                                 - tmp_df.ix[tmp_df.index[ind - 1], new_cols.get('Посетители (кросс-девайс)')]) \
                    /tmp_df.ix[tmp_df.index[ind - 1], new_cols.get('Посетители (кросс-девайс)')]*100
                    new_df['Динамика роста (браузер от кросс-девайc)'].ix[tmp_df.index[ind]] = (tmp_df.ix[tmp_df.index[ind], new_cols.get('Посетители (браузер)')] \
                                                                 - tmp_df.ix[tmp_df.index[ind - 1], new_cols.get('Посетители (кросс-девайс)')]) \
                    /tmp_df.ix[tmp_df.index[ind - 1], new_cols.get('Посетители (кросс-девайс)')]*100
                else:
                    pass
        new_df['Рост * процент в категории (браузер)'] = new_df['Динамика роста (браузер)'] *  new_df['Процент в категории (браузер)']
        new_df['Рост * процент в категории (кросс-девайс)'] = new_df['Динамика роста (кросс-девайс)'] * new_df['Процент в категории (кросс-девайс)']
        new_df['Рост * процент в категории (браузер от кросс-девайc)'] = new_df['Динамика роста (браузер от кросс-девайc)'] *  new_df['Процент в категории (браузер от кросс-девайс)']
        
        cats = {"beauty_vitamins_supplements": "Красота, Витамины",
                "beauty_healthy_lifestyle": "Красота, Зож",
                "healthcare_alternative_therapy": "Медицина, Альт. Медицина",
                "healthcare_disease": "Медицина, Болезни",
                "healthcare_vitamins_supplements": "Медицина, Витамины",
                "healthcare_medicines_healthcare_products": "Медицина, Лекарства",
                "healthcare_healthcare_services": "Медицина, Мед. Услуги"}
        
        new_df['Тематики ресурса'] = cats.get("_".join(topic.split("_2C")))
        return new_df
    tdf = get_growth(df, df['URL-адрес ресурса'].unique())
    tdf.to_pickle(outfile)

def main():
    folder = os.getcwd() + '\\data'
    files = os.listdir(folder)
    path = [os.path.join(folder, file) for file in files]

    topics = []
    for topic in files:
        if topic.endswith(".csv"):
            new_topic = "_".join(topic.split("_")[2:])
            if new_topic not in topics:
                topics.append(new_topic)

    for topic in topics:
        outfile = folder + '\\new' + '\\' + topic.replace(".csv", ".pkl")
        tmp_df = read_sts(path, topic)
        write_sts(tmp_df, outfile, topic.replace(".csv", ""))

if __name__ == '__main__':
    main()
