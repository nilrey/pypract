# errFiles = []
# errFiles.append("filename")
# errFiles.append("filename2")

# print("Ошибка: "+(",".join(errFiles) )+" не найдены")

class ColorLoop():    
    def __init__(self) :
        self.color_set = [ "6b8e23", "a0522d", "00ff00", "778899", "00fa9a", "000080", "00ffff", "ff0000", "ffa500", "ffff00", "0000ff", "ff00ff", "1e90ff", "ff1493", "ffe4b5"]
        self.main_loop = list(range (0, 20))

    def color_loop(self):
        color_count = len(self.color_set)
        for i in self.main_loop:
            print(f'{i} : {i%color_count} : {self.color_set[i%color_count]}')

    def get_color(self, i):
        color_count = len(self.color_set)
        return self.color_set[i%color_count]

if __name__ == "__main__":
    cl = ColorLoop()
    cl.color_loop()
    # print(cl.get_color(16))