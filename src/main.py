from extract.extract_eo import ExtractEO
from extract.extract_ciro import ExtractCIRO
from extract.extract_cmpo import ExtractCMPO
def main():
    
    #extractor = ExtractEO()
    #extractor.run()
    ExtractCMPO().run()
    #extractor = ExtractCIRO()
    #extractor.run()

if __name__ == "__main__":
    main()