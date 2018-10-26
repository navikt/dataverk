from pathlib import Path
import os

"""
TODO
1. Gi navn til det nye Prosjektet/Repoet
2. Velg lisens
3. Sett Codeowner

... 

N -2. Sjekk at git er installert
N - 1. git init
N. Installer alle mapper og filer i det nye repoet

"""
CODEOWNERS_FILE_CONTENT = "*       {}"


class NewRepoCreator:
    """ Klasse for å konfigurere og instansiere nytt repository for datapakker/datasett
    """

    def __init__(self, new_repository_name: str):
        # [TODO] skal vi sjekke for unike repo navn?
        self.name = new_repository_name

        #Dict for å holde config filer som skal lages i  formatet -> filepath: filecontent
        self.config_files_to_be_created = dict()
        self.valid_licences = self._load_licences()

    def create(self):
        """Konfigurasjon er ferdig, bygg repo med alle filene inkludert"""
        new_path = self.create_repository()
        self.create_config_files(new_path)

    def create_repository(self):
        new_repo_dir_path = Path(self.name)
        if new_repo_dir_path.exists():
            raise IsADirectoryError("Directory already exisits")
        else:
            new_repo_dir_path.mkdir()

            os.system("cd {}; git init".format(new_repo_dir_path.absolute()))
            return new_repo_dir_path

    def add_config_file(self, filepath: str, filecontent):
        """Legg  til en ny fil som skal skrives inn i det nye repoet

        @:param
        :parameter    filepath: pathen hvor filen skal ligge i det nye repoet
        :parameter    filecontent: innholdet eller pathen til filen som skal lages
        """

        new_file_path = Path(filepath)

        if new_file_path.exists():
            raise FileExistsError("File has already been created or already exists")

        if new_file_path in self.config_files_to_be_created.keys():
            raise FileExistsError("File has already been added")

        self.config_files_to_be_created[new_file_path] = filecontent

    def create_config_files(self, dir_to_create_in: Path):
        """ Lag alle registrerte filer i prosjekt mappen.

        @:param
        :parameter  dir_to_create_in: Root mappen for prosjektet

        """
        for filepath, content in self.config_files_to_be_created.items():
            if isinstance(content, Path):
                self.create_config_file_from_copy(dir_to_create_in.joinpath(filepath), content)
            else:
                self.create_config_file_from_string(dir_to_create_in.joinpath(filepath), content)

    def create_config_file_from_copy(self, createpath: Path, copypath: Path):
        """Lager en ny fil på createpath som kopi fra filen som ligger i copypath"""

        file_content = None
        with copypath.open("r") as copyer:
            file_content = copyer.read()

        # Skriv innholdet til fil
        with createpath.open("w+") as writer:
            writer.write(file_content)

    def create_config_file_from_string(self, filepath: Path, string:str):
        """Lager en ny fil på filepath av stringen som gis som parameter"""

        with filepath.open("w+") as writer:
            writer.write(string)

    def set_licence(self, licence_index: str):
        index = int(licence_index)

        if index in self.valid_licences:
            self.add_config_file("LICENCE.md", self.valid_licences[index])

    def _load_licences(self) -> dict:
        loaded_licences = dict()
        path = Path().resolve().parent

        loaded_licences[1] = path.joinpath("LICENSE.md")
        return loaded_licences


def main():
    print("Lag et nytt dataverk prosjekt")
    new_name = input("Vennligst skriv et nytt unikt prosjekt navn:")

    repo_creator = NewRepoCreator(new_name)

    #Legg til lisens
    licence_choice = input("velg en lisens type: \n1: MIT\n")
    repo_creator.set_licence(licence_choice)

    # Legg til CODEOWNERS
    codeowner = input("skriv Codeowner navn")
    codeowners_string = CODEOWNERS_FILE_CONTENT
    repo_creator.add_config_file("CODEOWNERS.md", codeowners_string.format(codeowner))

    # opprett det nye repoet med alle filene inkludert
    repo_creator.create()


if __name__ == "__main__":
    main()



