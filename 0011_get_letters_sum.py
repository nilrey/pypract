'''
https://leetcode.com/problems/sum-of-digits-of-string-after-convert/description/?envType=daily-question&envId=2024-09-03
1945. Sum of Digits of String After Convert
You are given a string s consisting of lowercase English letters, and an integer k.

First, convert s into an integer by replacing each letter with its position in the alphabet (i.e., replace 'a' with 1, 'b' with 2, ..., 'z' with 26). Then, transform the integer by replacing it with the sum of its digits. Repeat the transform operation k times in total.

For example, if s = "zbax" and k = 2, then the resulting integer would be 8 by the following operations:

    Convert: "zbax" ➝ "(26)(2)(1)(24)" ➝ "262124" ➝ 262124
    Transform #1: 262124 ➝ 2 + 6 + 2 + 1 + 2 + 4 ➝ 17
    Transform #2: 17 ➝ 1 + 7 ➝ 8

Return the resulting integer after performing the operations described above.
'''

import string

class CountLetters():

    def getLucky(self, s, k) -> None:
        """
        s = string
        k = int
        """
        self.abc = ' ' + string.ascii_lowercase # все буквы по порадку алфавита
        nums = "".join( list( map( lambda letter: str(self.abc.find(letter)), s ) ) )
        print(nums)
        for i in range(k):
            sum = 0 
            for num in  nums: sum += int(num)
            print(sum)
            if sum < 10: break
            else: 
                nums = str(sum)
        
        return sum

if __name__ == "__main__":
    c = CountLetters
    # CountLetters('abc', 1)
    print(c.getLucky(c, 'leetcode', 2))
