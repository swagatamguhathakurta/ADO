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
	if(remove(fname) == 0)
		return 1;
	else
		return 0;
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
    		printf("Storage manager is already initialized");
    	}
    else
    	printf("Storage manager is already initialized");
}

RC createPageFile (char *fileName)
{
	if (checkinit() == RC_OK)
	{
		if (exists(fileName))
		{
			return RC_FILE_PRESENT;
		}
		else
		{
			FILE *fp=fopen(fileName, "w");
			fseek(fp, PAGE_SIZE, SEEK_SET);
			fputc('\0', fp);
			fclose(fp);
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
				FILE *fp=fopen(fileName, "r");
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
	if (checkinit() == RC_OK)
	{
		if (exists(fileName))
		{
			delete(fileName);
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
	return RC_OK;
}

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	int fd = (int) fHandle->mgmtInfo;

	if (checkinit() == RC_OK)
		{
			if (exists(fHandle->fileName))
			{
				if((pageNum<fHandle->totalNumPages) && (pageNum>=0))
				{
					lseek(fd,(PAGE_SIZE*pageNum),SEEK_SET);
					read(fd,memPage,PAGE_SIZE);
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
	int fd = (int) fHandle->mgmtInfo;

	if (checkinit() == RC_OK)
			{
				if (exists(fHandle->fileName))
				{
					if(pageNum>=0)
					{
						lseek(fd,(PAGE_SIZE*pageNum),SEEK_SET);
							if(pageNum>fHandle->totalNumPages) //to create pages
							{
								fHandle->totalNumPages=pageNum+1;
							}
							write(fd,memPage,PAGE_SIZE);
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

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	writeBlock (fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	char emptyblock[PAGE_SIZE];
	emptyblock[0]='\0';
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
	char emptyblock[PAGE_SIZE];
	emptyblock[0]='\0';

		if (checkinit() == RC_OK)
			{
				if (exists(fHandle->fileName))
				{
					writeBlock (numberOfPages, fHandle, emptyblock);
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
