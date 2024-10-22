import { HttpStatus } from '@nestjs/common';
import { Request, Response, NextFunction } from 'express';

export function optionsMiddleware(
  req: Request,
  res: Response,
  next: NextFunction,
) {
  if (req.method == 'OPTIONS') {
    res.status(HttpStatus.OK);
    res.set({
      'Content-Type': 'text/plain',
      'Content-Length': '0',
    });
    res.send();
    res.end();
  } else next();
}
