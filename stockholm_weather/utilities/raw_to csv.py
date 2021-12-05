import os


def create_csv(path_to_file,path_to_out):
    for filename in os.listdir(path_to_file):
        if filename.endswith(".txt") :
            # print(os.path.join(directory, filename))
            print(filename)
            fin = open(path_to_file+'/'+filename, "rt")
            fout = open(path_to_out+'/'+filename.split('.',1)[0]+'.csv', "wt")

            for line in fin:
                x = ','.join(line.split())
                fout.write(x+'\n')

            fin.close()
            fout.close()


if __name__ == "__main__":

    path_to_file_temp ="./../raw_data/Temprature"
    path_to_out_temp = "./../raw_data_csv/Temprature/"
    path_to_file_ap ="./../raw_data/Air_pressure"
    path_to_out_ap = "./../raw_data_csv/Air_pressure/"
    if not os.path.exists(os.path.dirname(path_to_out_temp)):
        os.makedirs(os.path.dirname(path_to_out_temp))
    if not os.path.exists(os.path.dirname(path_to_out_ap)):
        os.makedirs(os.path.dirname(path_to_out_ap))

    create_csv(path_to_file_temp,path_to_out_temp)
    create_csv(path_to_file_ap,path_to_out_ap)

