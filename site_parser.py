# reaches to a page of questions on math stackexchange
# formats received HTML into a list of Question instances in ProcessPage

from typing import List
import bs4
import requests

class Question:
    def __init__(self) -> None:
        self.id = ""
        self.text = ""
        self.tags = []
        self.score = 0
        self.answers = 0
        self.views = 0
    
    def present(self) -> str:
        tagsString = ""
        for tag in self.tags:
            tagsString += f"#{tag} "
        return f"[{self.id}] {self.text} {tagsString} --- score: {self.score}; {self.answers} answers, {self.views} views"
    
def questionFromElement(element: bs4.BeautifulSoup) -> Question:
    question = Question()
    question.id = element.get("data-post-id")
    question.text = element.find(class_="s-link").contents[0]
    for cursor in element.findAll(class_="d-inline mr4 js-post-tag-list-item"):
        question.tags.append(cursor.find().contents[0])
    stats = element.findAll(class_="s-post-summary--stats-item-number")
    question.score = int(stats[0].contents[0])
    question.answers = int(stats[1].contents[0])
    question.views = int(stats[2].contents[0])
    return question

def ProcessPage(url : str) -> List[Question]:
    questions = bs4.BeautifulSoup(requests.get(url).text, features="html.parser").findAll(class_ = "s-post-summary")
    return [questionFromElement(element) for element in questions]
    
def GatherByTag(filterTag : str, scanDepth : int = 1000) -> List[str]:
    baseURL = "https://math.stackexchange.com/questions?tab=newest&pagesize=50&page="
    counter = 0
    pageNumber = 1
    filtered = []
    while counter < scanDepth:
        pageURL = baseURL + str(pageNumber)
        for question in ProcessPage(pageURL):
            if counter >= scanDepth:
                break
            if filterTag in question.tags or filterTag == "":
                filtered.append(question.text)
            counter += 1
        pageNumber += 1
    return filtered

        