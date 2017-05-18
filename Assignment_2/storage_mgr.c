#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "dberror.h"
#include "storage_mgr.h"


int init=0;

int checkinit (void)
{
    if (init == 1)
    	return RC_OK;
    else
    	return -1;
}

int exists(const char *fname)
{
	if( access( fname, F_OK ) != -1 ) {
		return 1;
	} else {
		return 0;
	}
}

int delete(const char *fname)
{
	int rrem=-1;
	rrem=remove(fname);
	return rrem;
}
/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
void initStorageManager (void)
{
    if (checkinit() != RC_OK)
    	{
    		init=1;
    		printf("Storage manager is initialized");
    	}
    else
    	printf("Storage manager is already initialized");
}

RC createPageFile (char *fileName)
{
	int i;
	char emptyblock[PAGE_SIZE];
	int writef;
	if (checkinit() == RC_OK)
	{
		if (exists(fileName))
		{
			return RC_FILE_PRESENT;
		}
		else
		{
			FILE *fp=fopen(fileName, "w");
			for(i=0;i<PAGE_SIZE;i++)
			{
				emptyblock[i]=0;
			}
			writef=fwrite(emptyblock, 1, PAGE_SIZE, fp);
			        if (writef < PAGE_SIZE)
			        {
			          fclose(fp);
			          return RC_WRITE_FAILED;
			        }
			return RC_OK;
		}
	}
	else
	{
		return RC_STORAGE_MGR_NOT_INIT;
	}
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
	if (checkinit() == RC_OK)
		{
			if (exists(fileName))
			{
				FILE *fp=fopen(fileName, "r+");
				fseek(fp, 0, SEEK_END);
				fHandle->totalNumPages = (int) (ftell(fp)/PAGE_SIZE);
				fseek(fp, 0, SEEK_SET);
				fHandle->curPagePos = 0;
				fHandle->fileName = fileName;
				fHandle->mgmtInfo = fp;
				return RC_OK;
			}
			else
			{
				return RC_FILE_NOT_FOUND;
			}
		}
		else
		{
			return RC_STORAGE_MGR_NOT_INIT;
		}
}

RC closePageFile (SM_FileHandle *fHandle)
{
	if (checkinit() == RC_OK)
		{
			if (exists(fHandle->fileName))
			{
				fclose(fHandle->mgmtInfo);
				fHandle->totalNumPages=-1;
				fHandle->curPagePos=-1;
				fHandle->curPagePos='\0';
				fHandle->mgmtInfo=NULL;
				return RC_OK;
			}
			else
			{
				return RC_FILE_NOT_FOUND;
			}
		}
		else
		{
			return RC_STORAGE_MGR_NOT_INIT;
		}
}

RC destroyPageFile (char *fileName)
{
	int rd;
	if (checkinit() == RC_OK)
	{
		if (exists(fileName))
		{
			rd=delete(fileName);
			if(rd==0)
			return RC_OK;
			else
			return RC_DELETE_FAILED;
		}
		else
		{
			return RC_FILE_NOT_FOUND;
		}
	}
	else
	{
		return RC_STORAGE_MGR_NOT_INIT;
	}
	return RC_OK;
}

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	FILE *fd=fHandle->mgmtInfo;
	int seekpage= PAGE_SIZE*pageNum;
	printf("%d\n", seekpage);
	int readp;
	int readl;
	int i;
	if (checkinit() == RC_OK)
		{
			if (exists(fHandle->fileName))
			{
				if((pageNum<fHandle->totalNumPages) && (pageNum>=0))
				{
					readl=fseek(fd,seekpage,SEEK_SET);
					printf("\nreadl:%d:", readl);
					readp=fread(memPage,1,PAGE_SIZE, fd);
					printf("\nreadp:%d", readp);
					printf("\n");
					for (i=0;i<PAGE_SIZE; i++)
						printf("%c", memPage[i]);
					if (readp < PAGE_SIZE)
				        return RC_READ_FAILED;
					fHandle->curPagePos=pageNum;
					return RC_OK;
				}
				else
				{
					return RC_READ_NON_EXISTING_PAGE;
				}
			}
			else
			{
				return RC_FILE_NOT_FOUND;
			}
		}
		else
		{
			return RC_STORAGE_MGR_NOT_INIT;
		}
}

int getBlockPos (SM_FileHandle *fHandle)
{
	if (checkinit() == RC_OK)
			{
				if (exists(fHandle->fileName))
				{
					return(fHandle->curPagePos);
				}
				else
				{
					return RC_FILE_NOT_FOUND;
				}
			}
	else
	{
				return RC_STORAGE_MGR_NOT_INIT;
	}
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(readBlock (0, fHandle, memPage));
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(readBlock ((fHandle->curPagePos-1), fHandle, memPage));
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(readBlock (fHandle->curPagePos, fHandle, memPage));
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(readBlock ((fHandle->curPagePos+1), fHandle, memPage));
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(readBlock (fHandle->totalNumPages-1, fHandle, memPage));
}

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
		FILE *fd=fHandle->mgmtInfo;
	int seekpage= PAGE_SIZE*pageNum;
	int readp;
	int readl;
	int i;

	if (checkinit() == RC_OK)
		{
			if (exists(fHandle->fileName))
			{
				if(pageNum>=0)
				{
					readl=fseek(fd,seekpage,SEEK_SET);
					if(pageNum>fHandle->totalNumPages) //to create pages
					{
						fHandle->totalNumPages=pageNum+1;
					}
					readp=fwrite(memPage,1,PAGE_SIZE, fd);
				    if ( readp< PAGE_SIZE)
				        return RC_WRITE_FAILED;
					fHandle->curPagePos=pageNum;
					return RC_OK;
				}
				else
				{
					return RC_INVALID_PAGE_NUMBER;
				}
			}
			else
			{
				return RC_FILE_NOT_FOUND;
			}
		}
		else
		{
			return RC_STORAGE_MGR_NOT_INIT;
		}
}


RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return(writeBlock (fHandle->curPagePos, fHandle, memPage));
}

RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	int i;
	char emptyblock[PAGE_SIZE];
	for(i=0;i<PAGE_SIZE;i++)
		emptyblock[i]=0;

	printf("\nLength of emptyblock that will be written:%d", strlen(emptyblock));

	if (checkinit() == RC_OK)
		{
			if (exists(fHandle->fileName))
			{
				writeBlock (fHandle->totalNumPages, fHandle, emptyblock);
				return RC_OK;
			}
			else
			{
				return RC_FILE_NOT_FOUND;
			}
		}
		else
		{
			return RC_STORAGE_MGR_NOT_INIT;
		}
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	int i;
		char emptyblock[PAGE_SIZE];
		for(i=0;i<PAGE_SIZE;i++)
			emptyblock[i]=0;

		if (checkinit() == RC_OK)
			{
				if (exists(fHandle->fileName))
				{
					writeBlock (numberOfPages-1, fHandle, emptyblock);
					return RC_OK;
				}
				else
				{
					return RC_FILE_NOT_FOUND;
				}
			}
			else
			{
				return RC_STORAGE_MGR_NOT_INIT;
			}
}
